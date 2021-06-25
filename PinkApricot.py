# -*- coding: utf-8 -*-
# @Time    : 2021/06/21 16:00
# @Author  : floatsliang@gmail.com
# @File    : __init__.py

"""PinkApricot is configurable sql exporter not only export metrics but also love :)"""

from __future__ import annotations

import getopt
import json
import re
import sys
import traceback
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED, Future
from contextlib import suppress
from copy import deepcopy
from datetime import datetime, timezone, timedelta, tzinfo
from threading import Lock
from typing import Dict, List, Optional, Iterable

import dsnparse
import pymysql
from flask import Flask, Response
from prometheus_client import Gauge, generate_latest, REGISTRY
import pytz

DEFAULT_LISTEN_ADDRESS = ":9270"
DEFAULT_METRICS_PATH = "/metrics"
DEFAULT_CONFIG_FILE = "PinkApricot.json"


class ReferencedFunctions(object):
    """
    embedded function referenced by exporter config.
    will be invoked internally and replace its
    placeholder with execution result
    """
    REFERENCE_HINT = "ref_"

    @staticmethod
    def ref_get_enabled_dome_mobile_gameid():
        pass

    @staticmethod
    def ref_get_enabled_dome_pc_gameid():
        pass

    @staticmethod
    def ref_get_enabled_global_mobile_gameid():
        pass


class Configs(object):
    """
    util functions to handle exporter configs
    """
    PLACEHOLDER = "__"

    NOW_PATTERN = re.compile(r"^now(?P<op>[-+])?(?P<num>\d+)?(?P<unit>[yMwdhHms])?(?P<floor>\[[lr]d])?\s*$")

    @staticmethod
    def check_config_key(config: Dict, requirement: Dict):
        """
        check existence, not null etc. constraint of config key
        :param config:
        :param requirement:
        :return:
        """
        for k, v in config.items():
            if k not in requirement:
                raise Exception("unknown config key: {}".format(k))
        for k, v in requirement.items():
            if v and (k not in config or config[k] is None):
                raise Exception("config key {} required but not provided".format(k))

    @staticmethod
    def is_placeholder(obj) -> (str, bool):
        return obj, obj.startswith(Configs.PLACEHOLDER) and obj.endswith(Configs.PLACEHOLDER) \
            if isinstance(obj, str) else None, False

    @staticmethod
    def contains_placeholder(obj) -> (str, bool):
        if isinstance(obj, str):
            head = obj.find(Configs.PLACEHOLDER)
            tail = obj.rfind(Configs.PLACEHOLDER)
            if head < 0 or tail < 0 or tail <= head + len(Configs.PLACEHOLDER):
                return None, False
            return obj[head: tail + len(Configs.PLACEHOLDER)], True
        else:
            return None, False

    @staticmethod
    def accept_query_params(query: str, params: Dict) -> str:
        """
        sql placeholder accept its parameters
        """
        if params is None or len(params) == 0:
            return query
        for k, v in params.items():
            ph = Configs.PLACEHOLDER + k + Configs.PLACEHOLDER
            query = query.replace(ph, str(v))
        return query

    @staticmethod
    def resolve_config_placeholder(ph: str, now: datetime) -> object:
        """
        placeholder is a symbol temporarily takes the place of the final data.
        final data is e.g. the execution result of ReferencedFunctions
        or other result resolved by PinkApricot

        :param ph: placeholder
        :param now: datetime of now
        :return: final data
        """
        key = ph[2: len(ph) - 2]

        # time type placeholder
        matches = Configs.NOW_PATTERN.match(key)
        if matches is not None:
            gd = matches.groupdict()
            date = None
            if len(gd) == 0:
                date = now
            elif len(gd) >= 3:
                neg = 1 if gd["op"] == "+" else -1
                num = int(gd["num"])
                if gd["unit"] == "s":
                    date = now + timedelta(seconds=neg * num)
                elif gd["unit"] == "m":
                    date = now + timedelta(minutes=neg * num)
                elif gd["unit"] == "h":
                    date = now + timedelta(hours=neg * num)
                elif gd["unit"] == "d":
                    date = now + timedelta(days=neg * num)
                else:
                    return date.strftime("%Y-%m-%d %H:%M:%S")
            if "floor" in gd:
                if gd["floor"] == "[ld]":
                    date = datetime(date.year, date.month, date.day, tzinfo=date.tzinfo)
                elif gd["floor"] == "[rd]":
                    date = date + timedelta(days=1)
                    date = datetime(date.year, date.month, date.day, tzinfo=date.tzinfo)
            return date.strftime("%Y-%m-%d %H:%M:%S")
        elif key.startswith(ReferencedFunctions.REFERENCE_HINT):
            if hasattr(ReferencedFunctions, key):
                try:
                    return getattr(ReferencedFunctions, key)()
                except Exception as ex:
                    print("failed to invoke embedded function [{}]: {}".format(key, ex))
            print("unknown embedded function referenced: {}".format(key))
        return None

    @staticmethod
    def merge_config(target: Dict, source: Dict) -> Dict:
        merged = {}
        merged.update(source)
        merged.update(target)
        return merged


class TargetConfig(object):
    """
    the target to monitor and the queries to execute on it
    """
    REQUIRED_CONFIG_KEY = {"data_source": True, "queries": True}

    def __init__(self, data_source: str, query_names: List[str]):
        self.data_source = data_source
        self.queries = query_names

    @staticmethod
    def load(config: Dict):
        Configs.check_config_key(config, TargetConfig.REQUIRED_CONFIG_KEY)
        return TargetConfig(config["data_source"], config["queries"])

    def init_db_connection(self) -> DBApi:
        if self.queries is None or len(self.queries) == 0:
            raise Exception("no queries to execute on data source {}".format(self.data_source))
        dsn = dsnparse.parse(self.data_source)
        if dsn.schemes[0] == "mysql":
            return MyDBApi(dsn)
        else:
            raise NotImplementedError("data source {} not implemented".format(dsn.schemes))


class QueryConfig(object):
    REQUIRED_CONFIG_KEY = {
        "name": True, "query": True, "collect_interval": False, "collect_point": False,
        "collect_once": False, "cache_last_collection": False, "is_template_query": False
    }

    def __init__(self, name: str, query: str, collect_interval: str = "auto",
                 collect_point: int = -1, collect_once: bool = False,
                 cache_last_collection: bool = False, is_template_query: bool = False):
        self.name: str = name
        self.collect_interval: str = collect_interval
        self.collect_point: int = collect_point
        self.collect_once: bool = collect_once
        self.cache_last_collection: bool = cache_last_collection
        self.query: str = query
        self.is_template_query: bool = is_template_query

    @staticmethod
    def load(config: Dict) -> QueryConfig:
        Configs.check_config_key(config, QueryConfig.REQUIRED_CONFIG_KEY)
        return QueryConfig(
            config["name"], config["query"], config.get("collect_interval", "auto"),
            config.get("collect_point", -1), config.get("collect_once", False),
            config.get("cache_last_collection", False), config.get("is_template_query", False)
        )

    def collect_in_auto_interval(self) -> bool:
        return self.collect_interval == "auto"

    def collect_in_day_interval(self) -> bool:
        return self.collect_interval == "day"

    def collect_in_hour_interval(self) -> bool:
        return self.collect_interval == "hour"

    def collect_in_minute_interval(self) -> bool:
        return self.collect_interval == "minute"


class QueryRef(object):
    REQUIRED_CONFIG_KEY = {"query_name": True, "values": True}

    def __init__(self, query_name: str, values: List):
        self.query_name: str = query_name
        self.values: List = values

    @staticmethod
    def load(config: Dict) -> QueryRef:
        Configs.check_config_key(config, QueryRef.REQUIRED_CONFIG_KEY)
        return QueryRef(config["query_name"], config["values"])


class MetricConfig(object):
    REQUIRED_CONFIG_KEY = {
        "name": True, "metric_type": True, "description": False, "key_labels": True,
        "static_labels": False, "val_label": False, "query_refs": True
    }

    def __init__(self, name: str, metric_type: str, description: str, key_labels: List[str],
                 static_labels: Dict, val_label: str, query_refs: List[QueryRef]):
        self.name = name
        self.metric_type = metric_type
        self.description = description
        self.key_labels = key_labels
        self.static_labels = static_labels
        self.val_label = val_label
        self.query_refs = query_refs

    @staticmethod
    def load(config: Dict) -> MetricConfig:
        Configs.check_config_key(config, MetricConfig.REQUIRED_CONFIG_KEY)
        query_refs = []
        for qr in config["query_refs"]:
            query_refs.append(QueryRef.load(qr))
        return MetricConfig(
            config["name"], config["metric_type"], config.get("desciption", ""),
            config["key_labels"], config.get("static_labels", {}),
            config.get("val_label", ""), query_refs
        )


class CollectorConfig(object):
    REQUIRED_CONFIG_KEY = {
        "metrics": True, "queries": True, "targets": True,
        "query_params": False, "query_mode": False,
        "max_query_workers": False, "time_zone": False
    }
    DEFAULT_TIME_ZONE = pytz.timezone("Asia/Shanghai")

    def __init__(self, metrics: List[MetricConfig], queries: List[QueryConfig],
                 targets: List[TargetConfig], query_params: Dict, query_mode: str = "async",
                 max_query_workers: int = 10, time_zone: timezone = DEFAULT_TIME_ZONE):
        self.metrics: List[MetricConfig] = metrics
        self.queries: List[QueryConfig] = queries
        self.targets: List[TargetConfig] = targets
        self.query_params: Dict = query_params
        self.query_mode: str = query_mode
        self.max_query_workers: int = max_query_workers
        self.time_zone: timezone = time_zone

    @staticmethod
    def load(config: Dict) -> CollectorConfig:
        Configs.check_config_key(config, CollectorConfig.REQUIRED_CONFIG_KEY)
        metrics = []
        queries = []
        targets = []
        for m in config["metrics"]:
            metrics.append(MetricConfig.load(m))
        for q in config["queries"]:
            queries.append(QueryConfig.load(q))
        for t in config["targets"]:
            targets.append(TargetConfig.load(t))
        return CollectorConfig(
            metrics, queries, targets,
            config.get("query_params", {}),
            config.get("query_mode", "async"),
            config.get("max_query_workers", 10),
            pytz.timezone(config.get("time_zone", "Asia/Shanghai"))
        )

    @staticmethod
    def get_resolved_placeholder(ph: str, cached_placeholder: Dict, now: datetime) -> object:
        """
        ger resolved placeholder by resolving it or from cache
        """
        if Configs.is_placeholder(ph)[1]:
            if ph in cached_placeholder:
                return cached_placeholder[ph]
            else:
                val = Configs.resolve_config_placeholder(ph, now)
                cached_placeholder[ph] = val
                return val
        else:
            cph, ok = Configs.contains_placeholder(ph)
            if ok:
                if cph in cached_placeholder:
                    return ph.replace(cph, str(cached_placeholder[ph]))
                else:
                    val = Configs.resolve_config_placeholder(cph, now)
                    cached_placeholder[cph] = val
                    return ph.replace(cph, str(val))
        return None

    def spawn_query_params(self, query_names: List[str]) -> Dict:
        """
        generate a list of sql parameters according to config
        """
        params = {}
        if self.query_params is None or len(self.query_params) == 0:
            return params
        now = datetime.now(tz=self.time_zone)

        common_params = deepcopy(self.query_params.get("common_params", {}))
        cached_placeholder = {}

        # resolve common params
        for k, v in common_params.items():
            if not isinstance(v, list):
                rv = self.get_resolved_placeholder(v, cached_placeholder, now)
                if rv is not None:
                    common_params[k] = rv
                continue
            for idx, param in enumerate(v):
                if isinstance(param, Dict):
                    for pk, pv in param.items():
                        rv = self.get_resolved_placeholder(pv, cached_placeholder, now)
                        param[pk] = param[pk] if rv is None else rv
                else:
                    rv = self.get_resolved_placeholder(param, cached_placeholder, now)
                    v[idx] = v[idx] if rv is None else rv

        for query_name in query_names:
            params[query_name] = {}
            # make a deepcopy to avoid modify raw config
            query_param = deepcopy(Configs.merge_config(common_params, self.query_params[query_name]))
            if len(query_param) == 0:
                print("failed to spawn {} config: spawn result is empty".format(query_name))
            first_loop = True
            for k, v in query_param.items():
                if not isinstance(v, list):
                    rv = self.get_resolved_placeholder(v, cached_placeholder, now)
                    if rv is not None:
                        v = rv
                    else:
                        continue
                new_params = []
                for param in v:
                    m = {}
                    if isinstance(param, Dict):
                        for pk, pv in param.items():
                            rv = self.get_resolved_placeholder(pv, cached_placeholder, now)
                            if rv is not None:
                                param[pk] = rv
                        m = param
                    else:
                        rv = self.get_resolved_placeholder(param, cached_placeholder, now)
                        if rv is not None:
                            param = rv
                        m[k] = param
                    if first_loop:
                        new_params.append(m)
                    else:
                        for hp in params[query_name]:
                            new_params.append(Configs.merge_config(hp, m))
                params[query_name] = new_params
                first_loop = False
        return params


class Config(object):
    REQUIRED_CONFIG_KEY = {"collector": False, "listen_address": False, "metric_path": False,
                           "config_source": False, "source_location": False, "config_name": False}

    def __init__(self, collector: CollectorConfig, listen_address: str, metric_path: str,
                 config_source: str, source_location: str, config_name: str):
        self.collector: CollectorConfig = collector
        self.listen_address: str = listen_address
        self.metric_path: str = metric_path
        self.config_source: str = config_source
        self.source_location: str = source_location
        self.config_name: str = config_name

    def refresh_collector_config(self):
        """
        refresh collector config

        configs in the same level will not be changed
        i.e. listen_address, metric_path. etc.
        """
        if self.config_source == "file":
            with open(self.source_location, encoding="utf-8") as file:
                config = json.load(file)
                self.collector = CollectorConfig.load(config["collector"])
        elif self.config_source == "mysql":
            dbi = MyDBApi(dsnparse.parse(self.source_location))
            self.collector = CollectorConfig.load(Config._load_from_mydb(dbi, self.config_name))
        else:
            raise Exception("failed to refresh collector config, "
                            "config source {} not supported".format(self.config_source))

    @staticmethod
    def load(config_path: str) -> Config:
        with open(config_path, encoding="utf-8") as file:
            config = json.load(file)
            Configs.check_config_key(config, Config.REQUIRED_CONFIG_KEY)
            source = config.get("config_source", "file")
            if source == "file":
                collector_config = config["collector"]
                source_location = config_path
            elif source == "mysql":
                source_location = config.get("source_location")
                dbi = MyDBApi(dsnparse.parse(source_location))
                collector_config = Config._load_from_mydb(dbi, config.get("config_name"))
            else:
                raise Exception("config source {} not supported".format(source))
            return Config(
                CollectorConfig.load(collector_config),
                config.get("listen_address", ""),
                config.get("metric_path", ""),
                source, source_location,
                config.get("config_name", "")
            )

    @staticmethod
    def _load_from_mydb(dbi: MyDBApi, config_name: str) -> Dict:
        res = dbi.query("SELECT config FROM PinkApricot.ExporterConfig WHERE config_name = '{}' "
                        "ORDER BY version DESC LIMIT 1".format(config_name), ())
        if res is None or len(res) == 0:
            raise Exception("no config named {} exists in mydb".format(config_name))
        return json.loads(res[0]["config"])["collector"]


class MetricFamily(object):
    """
    a family of metrics, sharing the same name, help, labels.
    with logic for populating its labels and values from query result.
    """

    def __init__(self, config: MetricConfig):
        self.config: MetricConfig = config
        self.const_label_values: List = list(config.static_labels.values())
        self.non_const_key_labels = list(config.key_labels)
        labels = list(config.static_labels.keys()) + self.non_const_key_labels
        if config.val_label is not None and len(config.val_label) > 0:
            labels.append(config.val_label)
        if config.metric_type == "gauge":
            self.metric = Gauge(config.name, config.description, labels)
        else:
            raise Exception("not supported metric type: {}".format(config.metric_type))

    def collect(self, query_name: str, row: Dict, query_param: Dict,
                cache: CachedMetricCollection, need_cache: bool):
        """
        populate values from query output dict
        """
        target_ref: Optional[QueryRef] = None
        for ref in self.config.query_refs:
            if ref.query_name == query_name:
                target_ref = ref
                break
        if target_ref is None:
            print("metric {} cannot reference query {}".format(self.config.name, query_name))
            return
        label_values = list(self.const_label_values)
        for label in self.non_const_key_labels:
            if label in row:
                val = row[label]
            else:
                val = query_param[label]
            label_values.append(val)

        for val_label_val in target_ref.values:
            if val_label_val not in row:
                continue
            if self.config.val_label is not None and len(self.config.val_label) > 0:
                label_values.append(val_label_val)
            metric_val = float(row[val_label_val])
            self.metric.labels(*label_values).set(metric_val)
            if need_cache:
                cache.cache(self.metric, label_values, metric_val)

    def close(self):
        REGISTRY.unregister(self.metric)


class CachedMetricCollection(object):
    """
    metric label values and field value cached in last collection
    """

    def __init__(self):
        self.metrics: List[Gauge] = []
        self.label_values_collection: List[List] = []
        self.value_collection: List = []

    def cache(self, metric: Gauge, label_values: List, value: object):
        self.metrics.append(metric)
        self.label_values_collection.append(label_values)
        self.value_collection.append(value)

    def expire(self):
        self.metrics = []
        self.label_values_collection = []
        self.value_collection = []

    def is_empty(self) -> bool:
        return len(self.metrics) == 0

    def collect_metrics_from_cache(self):
        for idx, metric in enumerate(self.metrics):
            metric.labels(self.label_values_collection[idx]).set(self.value_collection[idx])


class Query(object):
    """
    named query to collect raw data for metrics, referenced by:
     1. one or more metrics, through query_ref.
     2. one target, through queries.

    when it is a template query, it can spawn one or more queries
    to execute in a certain collect action.

    template query has two types of placeholder:
    1. __db__ which will resolved by PinkApricot with parameter
       named `db` in query_params before querying
    2. %(db)s which will resolved by db driver during querying
    """

    def __init__(self, config: QueryConfig):
        self.config: QueryConfig = config
        self.mfs: List[MetricFamily] = []
        self._lock: Lock = Lock()
        self.last_collect_point: int = 0
        self.last_collection: CachedMetricCollection = CachedMetricCollection()
        self._executor: Optional[ThreadPoolExecutor] = None
        self._target: Optional[TargetConfig] = None

    def set_metric_families(self, mfs: List[MetricFamily]):
        self.mfs = mfs

    def set_executor(self, executor: ThreadPoolExecutor):
        self._executor = executor

    def set_target(self, target: TargetConfig):
        self._target = target

    @property
    def target(self) -> TargetConfig:
        return self._target

    @property
    def query_name(self) -> str:
        return self.config.name

    @property
    def cache_last_collection(self) -> bool:
        return self.config.cache_last_collection

    def has_cached_collection(self) -> bool:
        return self.last_collection.is_empty()

    def need_collect(self) -> bool:
        """
        return True if the query should not be skipped in this collection
        """
        now = datetime.now()
        if self.config.collect_in_auto_interval():
            return True
        elif self.config.collect_in_minute_interval():
            cur_collect_point = "{}-{}-{}-{}".format(now.year, now.month, now.day, now.hour)
            return self._check_collect_point(now.minute, cur_collect_point)
        elif self.config.collect_in_hour_interval():
            cur_collect_point = "{}-{}-{}".format(now.year, now.month, now.day)
            return self._check_collect_point(now.hour, cur_collect_point)
        elif self.config.collect_in_day_interval():
            cur_collect_point = "{}-{}".format(now.year, now.month)
            return self._check_collect_point(now.day, cur_collect_point)
        else:
            return True

    def _check_collect_point(self, now: int, cur_collect_point: str) -> bool:
        with self._lock:
            if self.config.collect_once:
                if cur_collect_point == self.last_collect_point:
                    return False
                if now == self.config.collect_point:
                    self.last_collect_point = cur_collect_point
                    return True
                return False
            else:
                return now == self.config.collect_point

    def collect(self, params: List[Dict]) -> Optional[List[Future]]:
        """
        1. spawn template query(if it is)
        2. execute query
        3. collect metrics
        :param params: sql params to fill in
        :return: list of asynchronous tasks or None
        """
        if (params is None or len(params) == 0) and self.config.is_template_query:
            print("template query {} with no params provided".format(self.query_name))
            return None

        # expire previous cached collection
        self.last_collection.expire()

        # queries to execute
        queries = []
        if not self.config.is_template_query:
            queries.append(self.config.query)
            params = [{}]
        else:
            for param in params:
                queries.append(Configs.accept_query_params(self.config.query, param))

        if self._executor is None:
            # no executor found, we do query sequentially
            for idx, query in enumerate(queries):
                try:
                    self.run(query, params[idx])
                except Exception as ex:
                    print("failed to execute query sequentially: {}".format(ex))
            return None
        else:
            # else query asynchronously
            tasks = []
            for idx, query in enumerate(queries):
                tasks.append(self._executor.submit(self.run, query, params[idx]))
            for task in tasks:
                task.add_done_callback(self.on_exception)
            return tasks

    def collect_from_cache(self):
        """
        reuse cached last collection and won't do a actual query
        """
        self.last_collection.collect_metrics_from_cache()

    def run(self, query: str, param: Dict):
        """
        run actual query and collect metrics from result
        :param query: sql to execute
        :param param: sql param to fill in
        """
        rows = self._target.init_db_connection().query(query, param)
        with self._lock:
            for row in rows:
                for mf in self.mfs:
                    mf.collect(
                        self.query_name, row, param,
                        self.last_collection,
                        self.cache_last_collection
                    )

    @staticmethod
    def on_exception(task: Future):
        """
        callback to handle async task exception
        :param task: task to execute
        """
        if task.exception():
            print("failed to execute query asynchronously: {}".format(task.exception()))


class Collector(object):
    """
    collector is a named set of related metrics that are collected together.
    """

    def __init__(self, config: CollectorConfig):
        self.config: CollectorConfig = config
        self.queries: List[Query] = []
        named_queries = {}
        # init queries
        for query_config in config.queries:
            query = Query(query_config)
            self.queries.append(query)
            named_queries[query_config.name] = query

        # init targets
        for target_config in config.targets:
            for query_name in target_config.queries:
                if named_queries[query_name].target is not None:
                    raise Exception("one query not supported execute on multiple target")
                named_queries[query_name].set_target(target_config)

        self.query_mfs: Dict[Query, List[MetricFamily]] = {}

        # init metrics
        for metric_config in config.metrics:
            mf = MetricFamily(metric_config)
            for ref in metric_config.query_refs:
                if ref.query_name not in named_queries:
                    print("metric referenced non-exist query")
                    return
                query = named_queries[ref.query_name]
                mfs = self.query_mfs.get(query, [])
                self.query_mfs[query] = mfs
                mfs.append(mf)

        # init query executor(if query_mode is async)
        self._executor: Optional[ThreadPoolExecutor] = None
        if config.query_mode == "async":
            self._executor = ThreadPoolExecutor(max_workers=config.max_query_workers)

        for query, mfs in self.query_mfs.items():
            query.set_metric_families(mfs)
            query.set_executor(self._executor)

    def gather(self):
        """
        collect all configured metrics once
        """
        queries = []
        query_names = []
        cached_queries = []
        for q in self.query_mfs:
            if q.need_collect():
                # query should be executed in this gathering
                queries.append(q)
                query_names.append(q.query_name)
            elif q.has_cached_collection():
                # query skipped but could reuse cached last collection
                cached_queries.append(q)

        tasks = []
        params = self.config.spawn_query_params(query_names)
        for q in queries:
            ret = q.collect(params[q.query_name])
            if ret is not None:
                tasks.extend(ret)
        # wait for all query completed
        if len(tasks) > 0:
            wait(tasks, timeout=30, return_when=ALL_COMPLETED)
        # collect cached last collection
        for cq in cached_queries:
            cq.collect_from_cache()

    def close(self):
        for _, metrics in self.query_mfs.items():
            for metric in metrics:
                metric.close()


class DBApi(object):
    """
    db operation interface
    """

    def clone(self):
        raise NotImplementedError()

    def close(self):
        raise NotImplementedError()

    def query_many(self, sql: str, param: Iterable, t=None):
        raise NotImplementedError()

    def query_one(self, sql: str, param: Iterable, t=None):
        raise NotImplementedError()

    def query(self, sql: str, param: Iterable, t=None):
        raise NotImplementedError()


class MyDBApi(DBApi):
    """
    mysql db operation interface implemented DBApi
    """
    DEFAULT_CONFIG = {
        'host': 'localhost',
        'user': 'root',
        'db': 'PinkApricot',
        'charset': 'utf8',
        'autocommit': 0,  # default 0
        'cursorclass': pymysql.cursors.DictCursor
    }

    def __init__(self, dsn: dsnparse.ParseResult = None):
        self.dsn = dsn
        if dsn:
            self.config: Dict = {
                "host": dsn.host,
                "user": dsn.user,
                "cursorclass": pymysql.cursors.DictCursor
            }
            if dsn.port is not None:
                self.config["port"] = dsn.port
            if dsn.dbname is not None:
                self.config["db"] = dsn.dbname
            if dsn.password is not None:
                self.config["password"] = dsn.password
        else:
            self.config: Dict = MyDBApi.DEFAULT_CONFIG.copy()
        self.conn = pymysql.connect(**self.config)

    def clone(self):
        return MyDBApi(self.dsn)

    @classmethod
    def _log(cls, sql: str, param: Iterable):
        exec_sql = sql % param
        print(exec_sql)

    def close(self):
        self.conn.ping(reconnect=False)
        return self.conn.close()

    def query_many(self, sql: str, param: Iterable, t=None):
        _conn = t or self.conn
        reconnect = False if t else True
        try:
            _conn.ping(reconnect=reconnect)
            with _conn.cursor() as cursor:
                cursor.execute(sql, param)
                results = cursor.fetchall()
        except Exception as ex:
            raise ex
        self._log(sql, param)
        return results

    def query_one(self, sql: str, param: Iterable, t=None):
        _conn = t or self.conn
        reconnect = False if t else True
        try:
            _conn.ping(reconnect=reconnect)
            with _conn.cursor() as cursor:
                cursor.execute(sql, param)
                result = cursor.fetchone()
        except Exception as ex:
            raise ex
        self._log(sql, param)
        return result

    def query(self, sql: str, param: Iterable, t=None):
        return self.query_many(sql, param, t=t)


if __name__ == '__main__':
    opts, args = getopt.getopt(sys.argv[1:], "hl:p:c:", ["help", "listen-address=", "metric-path=", "config-file="])

    listen_addr = DEFAULT_LISTEN_ADDRESS
    metrics_path = DEFAULT_METRICS_PATH
    config_file = DEFAULT_CONFIG_FILE
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print("python sql_exporter.py -l xx -p xx -c xx")
            exit(0)
        elif opt in ("-l", "--listen-address"):
            listen_addr = arg
        elif opt in ("-p", "--metric-path"):
            metrics_path = arg
        elif opt in ("-c", "--config-file"):
            config_file = arg
    host_port = listen_addr.split(":")
    host = "localhost"
    port = 9270
    if len(host_port) == 1:
        # listen_addr == "127.0.0.1" style
        host = host_port[0]
    elif len(host_port) == 2:
        # listen_addr == "127.0.0.1:9270" or ":9270" style
        host = "localhost" if not host_port[0] else host_port[0]
        port = host_port[1]
    else:
        raise Exception("invalid listen address: {}".format(listen_addr))

    print("Starting PinkApricot Exporter...")
    # unregister internal metrics
    for cn in list(REGISTRY._names_to_collectors.values()):
        with suppress(KeyError):
            REGISTRY.unregister(cn)

    # initial exporter
    exporter_config = Config.load(config_file)
    if exporter_config.listen_address is None or len(exporter_config.listen_address) == 0:
        exporter_config.listen_address = listen_addr
    if exporter_config.metric_path is None or len(exporter_config.metric_path) == 0:
        exporter_config.metric_path = metrics_path
    exporter = Collector(exporter_config.collector)

    # initial http app
    app = Flask(__name__)


    @app.route(exporter_config.metric_path)
    def exporter_handler():
        """
        prometheus pull request handler
        :return: metrics
        """
        exporter.gather()
        return Response(generate_latest(), mimetype="text/plain")


    @app.route("/health")
    def health():
        """
        health check handler
        :return: i'm ok
        """
        return Response("OK")


    @app.route("/config/refresh")
    def refresh_config():
        """
        refresh exporter config and create a new exporter with it
        """
        global exporter
        exporter_config.refresh_collector_config()
        exporter.close()
        new_exporter = Collector(exporter_config.collector)
        exporter = new_exporter
        return Response("refreshed!")


    print("Listening on {}:{}".format(host, port))
    # start app
    app.run(host=host, port=port)
