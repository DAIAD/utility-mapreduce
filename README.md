# Introduction

MapReduce jobs for aggregating real and forecasting water consumption data. The jobs read/write data from/to HBase tables. Moreover, two files with user and group data respectively are required. These files must be stored in the folder described by the `daiad.mapreduce.job.local.cache` parameter and will be cached locally by YARN using the [addCacheFile](https://hadoop.apache.org/docs/r2.6.0/api/org/apache/hadoop/mapreduce/Job.html#addCacheFile(java.net.URI)) method.

# Input files

The user input file contains a row for each user who has a registered smart water meter (SWM). A row consists of a list of semi-colon delimited values. Each row contains the following fields:

* Serial number of the registered SWM
* User unique key of type [UUID](https://docs.oracle.com/javase/7/docs/api/java/util/UUID.html)
* Username
* User first name
* User last name

`I11FA555555;31926214-48b3-4905-b7a8-17a9c50ba0c6;user@daiad.eu;Demo;User`

The group input file controls how user data is aggregated. A row represents a member of a group and it consists of the following semi-colon delimited values:

* Group type. Valid values are `UTILITY`, `SEGMENT`, `COMMONS`, `SET` and `AREA`
* Group unique key of type [UUID](https://docs.oracle.com/javase/7/docs/api/java/util/UUID.html)
* Optional area unique key of type [UUID](https://docs.oracle.com/javase/7/docs/api/java/util/UUID.html)
* Serial number of a registered SWM
* Data time zone

```
UTILITY;941be15c-a8ea-40c9-8502-9b790d2a99f3;;I11FA555555;Europe/Athens
AREA;941be15c-a8ea-40c9-8502-9b790d2a99f3;9453a86e-4cfe-40a9-b5fb-c681dc31cac6;I11FA555555;Europe/Athens
```

# HBase tables

Details about the required schema for the HBase input and output tables can be found in the implementation of the mapper and reducer classes

| Table | Class |
| --------- | ----------- |
| Meter data aggregation input | [MeterAggregatorMapper](https://github.com/jkouvar/hbase-mapreduce/blob/master/src/main/java/eu/daiad/mapreduce/hbase/mapper/MeterAggregatorMapper.java#L161) |
| Meter data aggregation output | [MeterAggregatorReducer](https://github.com/jkouvar/hbase-mapreduce/blob/master/src/main/java/eu/daiad/mapreduce/hbase/reducer/MeterAggregatorReducer.java#L125) |
| Forecasting data aggregation input | [MeterForecastingAggregatorMapper](https://github.com/jkouvar/hbase-mapreduce/blob/master/src/main/java/eu/daiad/mapreduce/hbase/mapper/MeterForecastingAggregatorMapper.java#L161) |
| Forecasting data aggregation output | [MeterForecastingAggregatorReducer](https://github.com/jkouvar/hbase-mapreduce/blob/master/src/main/java/eu/daiad/mapreduce/hbase/reducer/MeterForecastingAggregatorReducer.java#L125) |

# Job parameters

Required MapReduce job parameters:

| Parameter | Description | Example |
| --------- | ----------- | ----------- |
| fs.defaultFS                    | The HDFS endpoint. | hdfs://namenode.local:9000 |
| mapreduce.framework.name        | The runtime framework for executing MapReduce jobs. Can be one of `local` or `yarn`. | yarn |
| yarn.resourcemanager.hostname   | The YARN resource manager host | resourcemanager.local |
| hbase.zookeeper.quorum          | Comma separated list of servers in the ZooKeeper Quorum. | host1.local,host2.local,host3.local |
| hbase.client.scanner.caching    | Number of rows that will be fetched when calling next on a scanner if it is not served from (local, client) memory. Higher caching values will enable faster scanners but will require more memory and some calls of next may take longer times when the cache is empty.    | 1000 |
| daiad.hbase.table.input         | HBase input table. | daiad:meter-data |
| daiad.hbase.table.output        | HBase output table. | daiad:meter-data-aggregate |
| daiad.hbase.column-family       | Default column family for input/output tables | cf |
| daiad.hbase.data.partitions     | Number of partitions used for partitioning time-series data. | 5 |
 | daiad.interval.format          | Date interval format. | yyyyMMdd |
| daiad.top.query.limit           |  Top-k / Bottom-k query limit. | 10 |
| daiad.interval.from             | Date interval start instant. | 20170101 |
| daiad.interval.to               | Date interval end instant. | 20170731 |
| daiad.filename.groups           | Groups file cached locally by YARN. | groups.csv |
| daiad.filename.users            | Users file cached locally by YARN. | users.csv |
| daiad.mapreduce.job.local.tmp   | Local temporary folder | |
| daiad.mapreduce.job.hdfs.tmp    | HDFS temporary folder | |
| daiad.mapreduce.job.local.file  | Optional local folder with files to copy to HDFS | |
| daiad.mapreduce.job.hdfs.file   | Optional HDFS folder to copy local files | |
| daiad.mapreduce.job.local.cache | Optional local folder with files to cache using the [addCacheFile](https://hadoop.apache.org/docs/r2.6.0/api/org/apache/hadoop/mapreduce/Job.html#addCacheFile(java.net.URI)) method.| |
| daiad.mapreduce.job.hdfs.cache  | Optional HDFS folder to copy cached files | |
| daiad.mapreduce.job.local.lib   | Optional local folder with libraries to add to the class path | |
| daiad.mapreduce.job.hdfs.lib    | Optional HDFS folder to copy libraries that will be added to the class path | |
| daiad.mapreduce.job.name        | Job name. Value values are `meter-data-pre-aggregation` and `meter-forecasting-data-pre-aggregation` | |

# Build

`mvn clean package`
