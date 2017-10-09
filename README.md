# smack-exercise [![Build Status](https://travis-ci.org/kbelova/smack-exercise.svg?branch=master)](https://travis-ci.org/kbelova/smack-exercise)

It's an example application  reading JSON tar.gz data with Spark, process, and publish to Cassandra in tabular view, nearly without IO FileSystem operation and network overhead. Components of Smack being used: spark, cassandra. Added Travis CI support and Docker container.

## How to run with Docker
0. Cassandra service should be up and running, versions: 2.1.5*, 2.2 or 3.0, before application execution;
1. Update file `./src/main/resources/application.conf`
```
cassandra {
  host="YOUR_CASSANDRA_HOST_NAME"
  port=9042
  username=""
  password=""
```
2. Create directory `./data` and put your yelp_dataset.tar.gz inside;
2. Edit TAR_PATH inside Dockerfile point it to {your_name}.tar.gz, by default, it's expecting: `yelp_dataset.tar.gz`;
3. Build and run docker image.

## Execution time and monitoring
It took about 20min for application to run on Intel core i7 with 16GB RAM in local[*] mode (spark uses 8 processes) and local cassandra cluster.
To monitor running app, possible to use: http://YOUR_SPARK_DRIVER_NAME:4040/jobs/

## Algorithm
Application start on driver 3 threads:
1. Uncompressing streamer;
2. Runnable with message queue, which broadcast on localhost:9999;
3. Main thread, where spark stream is listening to localhost:9999, transform and write messages into cassandra db.

Task itself says nothing about network, time execution restriction, process reliability, and possible loss amount. So I have made several assumption:
- than faster, than better;
- reduce IO to File System;
- reduce network overhead;
- medium reliably;

Spark is not able to uncompress/read .tar.gz in parallel mode (means it will be any way executed on a driver), as well as, waiting while archive will be uncompressed and after it read `*.json` into DataSet/DataFrame sounds like wasting a time.
Thats why I have implemented runnable with message queue on localhost. That was fast solution without network or IO overhead, but not very reliable: if something happend with driver, all message queue will be lost and calculation should be restarted. Here is a room for improvment: it is possilbe to add local kafka on driver node. Since sending ~6Gb of data over network on kafka cluster and almost immediately get it back for transformation doesn't sounds nice.

## Check data integrity
1. during execution, check failed jobs at: http://YOUR_SPARK_DRIVER_NAME:4040/jobs/
2. after execution in terminal:
* Test keyspace exists
```bash
$CASSANDRA_HOME/bin/cqlsh
cqlsh> describe keyspaces;
    system_schema  system_auth  system  system_distributed  test  system_traces
```
* Check that all tables are in place
```bash
cqlsh> use test;
cqlsh:test> describe tables;
business  checkin  tip  photos  user  review
```

*   To check creation query
```bash
cqlsh:test> describe table photo;
```
*   Check visually that all fields are populated correctly
```bash
cqlsh:test> select * from photos limit 30;
cqlsh:test> select * from user limit 30;
cqlsh:test> select * from tip limit 30;
cqlsh:test> select * from checkin limit 30;
cqlsh:test> select * from review limit 30;
cqlsh:test> select * from business limit 30;
```

* Bad practise in general, only for small tables - it tries to read all table in-memory.
Won't work for user and review tables.
```bash
cqlsh:test> select count(business_id) from photos;
cqlsh:test> select count(tip_id) from tip;
cqlsh:test> select count(checkin_id) from checkin;
cqlsh:test> select count(business_id) from business;
cqlsh:test> quit
```
* Returns statistics on tables in human-readable format
```bash
$CASSANDRA_HOME/bin/nodetool tablestats test.tip -H
$CASSANDRA_HOME/bin/nodetool tablestats test.user -H
$CASSANDRA_HOME/bin/nodetool tablestats test.review -H
$CASSANDRA_HOME/bin/nodetool tablestats test.business -H
$CASSANDRA_HOME/bin/nodetool tablestats test.checkin -H
$CASSANDRA_HOME/bin/nodetool tablestats test.photos -H
Total number of tables: 42
----------------
Keyspace : test
        Read Count: 0
        Read Latency: NaN ms.
        Write Count: 0
        Write Latency: NaN ms.
        Pending Flushes: 0
                Table: photos
                SSTable count: 1
                Space used (live): 9.6 MiB                  <- total space table use on a disk
                Space used (total): 9.6 MiB
                Space used by snapshots (total): 0 bytes
                Off heap memory used (total): 42.69 KiB
                SSTable Compression Ratio: 0.7588283166829642
                Number of keys (estimate): 27561                        <- amount of partitions
                Memtable cell count: 0
                Memtable data size: 0 bytes
                Memtable off heap memory used: 0 bytes
                Memtable switch count: 0
                Local read count: 0
                Local read latency: NaN ms
                Local write count: 0
                Local write latency: NaN ms
                Pending flushes: 0
                Percent repaired: 0.0
                Bloom filter false positives: 0
                Bloom filter false ratio: 0.00000
                Bloom filter space used: 34.01 KiB
                Bloom filter off heap memory used: 34 KiB
                Index summary off heap memory used: 7.24 KiB
                Compression metadata off heap memory used: 1.45 KiB
                Compacted partition minimum bytes: 73
                Compacted partition maximum bytes: 61214
                Compacted partition mean bytes: 479
                Average live cells per slice (last five minutes): NaN
                Maximum live cells per slice (last five minutes): 0
                Average tombstones per slice (last five minutes): NaN
                Maximum tombstones per slice (last five minutes): 0
                Dropped Mutations: 0 bytes

----------------
```