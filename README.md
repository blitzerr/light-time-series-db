# light-time-series-db

My attempt at writing a high performance and lightweight time series database.

Timeseries data has a few features that makes implementing such a database simpler than a traditional
relationo database.

1. A time series data has a source(ID), a metric (ID). This 2-tuple, helps to identify a series uniquely. And then there are timestamped data points that form the actual data. Each data instance is called a data-point and the uniqueness is guaranteed by the timestamp. The system can choose how it wants to treat repeated data-points. A usual strategy would be to throw away the next occurrence of the same data point. Time series are usually sensory data and therefore, a sensor because of some error can choose to emit the same data point twice or send data-points out of order.

1. The out of order data points creates a problem. A timeseries database is usually tolerant of datapoints slightly out of order which is a configurable window. Any data-point that is older that the window time is discarded. One might ask if this is configurable, why don't we configure this window to be 24- hours or a month. Well, there is no free lunch. Because the system has to do work to re-order out of order data-points, it likes to keep them in memory. Usually two such slabs of data is kept in memory because the window is determined by the server and the server can receive a data-point, from the last window, close to the window-rollover mark. In this case to honor the servers, contract of guaranteeing admittance of data points that are within the window duration, this must be accepted and any data-point that is accepted must be stored is timestamp order. Therefore, if one selects a window too large, one must be willing to pay the proportional cost in terms of memory.

1. The database is write heavy.

1. The data is ingested more or less is a time order, which also happens to be the primary key for such a database.

1. Past data is largely immutable but there are cases, where users might like the ability to bulk edit. The edits are usually in terms specifying a new field/tag to be indexed on but not changing the values of the fields or adding or removing new fields.

1. Querying is rare and usually done over a recent window of time (mostly to plot dashboards and alerts). Queries spanning multiple series ((source, metric) tuple) is quite common.

## Data Model
### Metric Types
|Name|timestamp|Value|Tags
|----|---------|-----|----|
|http.post.count|5649799|23|host=A;version=1.0.9
|http.post.count|5649756|23|host=B;version=1.0.9
|http.get.count|5649799|2|host=M,N;version=1.0.9
|unix.cpu.load|5649799|0.34|host=23,45;linux=5.28

### Log types
|Name|timestamp|Value|Tags
|----|---------|-----|----|
|apachelogs|5649799|{path="", level= "", message=""}|host=A,B;version=1.0.9
|postgres.logs|5649799|{path="", level= "", message=""}|host=A,B;version=1.0.9

The system is required to handle queries like

select avg(http.*) WITH time between (t1, t2) bucketized over 5seconds and (host=A || host=M) group by version.

## Usage

### Configurations

The configurations are specified in the `config/default.toml`.
By default the server runs on `http://localhost:9200`. You can configure the host and port in the config.

### Ingest

```json
curl -X POST "localhost:9200/<source>/<metric>?pretty" -H 'Content-Type: application/json' -d'
[
    {"timestamp": "<unix epoch milliseconds>", "value": 23.345},
    {"timestamp": "<unix epoch milliseconds>", "value": 29.345}
]
'
```



## Design

-- TBD

## References

### Papers

- [Facebook Gorilla](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf)
- [BTrDB by Michael P Andersen](https://www.usenix.org/system/files/conference/fast16/fast16-papers-andersen.pdf)

### Blogs

- [Blog by @xaprb](https://www.xaprb.com/blog/2014/06/08/time-series-database-requirements/)
- [Blog by @fabxc](https://fabxc.org/tsdb/)
- [Blog by @nakabonne](https://nakabonne.dev/posts/write-tsdb-from-scratch/?utm_source=pocket_mylist)
- [Blog by Ganesh](https://ganeshvernekar.com/blog/prometheus-tsdb-the-head-block/)

### Code

- [Prometheus](https://github.com/prometheus/prometheus/tree/main/tsdb)
- [InfluxDB-IOX](https://github.com/influxdata/influxdb_iox)
