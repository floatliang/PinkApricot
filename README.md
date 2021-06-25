# PinkApricot

PinkApricot is a configurable sql exporter not only export metrics but also love :)

### Usage

Run it from the command line:

```shell
python PinkApricot.py
```

Use the `--help` flag to get help information:

```shell
python PinkApricot.py --help
--listen-address -l string
    Exporter listen address. (default ':9270')
--metric-path -p string
    Prometheus pull metric path
--config-file -c string
    Exporter config file location
```

### Configuration

PinkApricot is a fully configurable sql exporter supplied with following features:

* multi metrics/queries supported.
* decoupling between metrics and queries, which means one metric could collect values from multi queries and one query could been collected by mutl metrics.
* configurable collect frequency.
* template query, queries can 

#### metric

#### query

* **template query**

#### target

#### query parameters

