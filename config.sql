BEGIN;

CREATE DATABASE PinkApricot;
USE PinkApricot;
DROP TABLE IF EXISTS ExporterConfig;

CREATE TABLE `ExporterConfig` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `config_name` varchar(63) NOT NULL COMMENT '配置名称',
  `version` int NOT NULL COMMENT '配置版本',
  `config` json NOT NULL COMMENT '配置内容',
  `createtime` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间戳',
  `updatetime` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间戳',
  PRIMARY KEY (`id`),
  UNIQUE KEY `config_name_version` (`config_name`, `version`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

INSERT INTO ExporterConfig (`config_name`, `version`, `config`) VALUES('PinkApricot', 2, '{"listen_address":":9270","metric_path":"/metrics","config_source":"mysql","source_location":"mysql://root:root@localhost:3306/PinkApricot","config_name":"PinkApricot","collector":{"query_mode":"async","max_query_workers":10,"metrics":[{"name":"mysql_slow_log_count","metric_type":"gauge","description":"Number of slow query","key_labels":["db","user_host","time_range"],"val_label":"query_time","query_refs":[{"query_name":"slow_query_count","values":["exceed_10s","exceed_60s"]}]}],"queries":[{"name":"slow_query_count","is_template_query":true,"query":"SELECT db as db, user_host as user_host, __time_range__ as time_range, COUNT(*) as __query_time_label__ FROM mysql.slow_log WHERE start_time > __start_time__ AND query_time > __query_time__ GROUP BY db, user_host"}],"targets":[{"data_source":"mysql://root:root@localhost:3306/mysql","queries":["slow_query_count"]}],"query_params":{"slow_query_count":{"time_range":[{"time_range":"1h","start_time":"__now-1h__"},{"time_range":"24h","start_time":"__now-24h__"}],"query_time":[{"query_time_label":"exceed_10s","query_time":"00:00:10"},{"query_time_label":"exceed_60s","query_time":"00:01:00"}]}}}}');

COMMIT;