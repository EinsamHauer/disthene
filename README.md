disthene: cassandra backed metric storage *writer*
==================================================

This project is inspired by [cyanite](https://github.com/pyr/cyanite) and is intended to replace it in write mode only. 
It must be fully compatible with **cyanite** and cyanite can still be used for reading. I've started another project 
which will do the reads. And, yes, I'm convinced that in a large scale setup (and that's why I started **disthene** project) it's best to separate these two roles. 

## Motivation

There are a couple of things which seem to be an absolute must and which were missing in **cyanite**:

* aggregation: ability to sum similar metrics from several servers
* blacklisting: ability to omit storing metrics which match a certain pattern. Makes not much sense by itself but is quite handy when you have previous item
* caching incoming paths: this may really reduce the load on Elasticsearch cluster
* some minimal set of own metrics (received count, write count, etc)
* true multitenancy

The other thing is performance. **Disthene** is being developed with one major requirement in mind - performance. 
Essentially, in most cases the only bottleneck will be C\*. 
There is no ultimate benchmark here (mostly because of the C\* bottleneck in test lab) 
but it looks like something like 1M data points/core/minute should not be a problem     

## Compiling 

This is a standard Java Maven project. 

```
mvn package
```

will most probably do the trick.

## Running

First of all, it's strongly recommended to run it with Java 8. Even though this software is fully compatible with Java 7. 
The main reason for that is a bug in Java ([JDK-7032154](http://bugs.java.com/view_bug.do?bug_id=7032154)) prior to version 8
 
There are a couple of things you will need in runtime, just the same set as for **cyanite**

* Cassandra
* Elasticsearch

Cassandra schema is identical to that of **cyanite**:

```
CREATE TABLE metric (
  period int,
  rollup int,
  tenant text,
  path text,
  time bigint,
  data list<double>,
  PRIMARY KEY ((tenant, period, rollup, path), time)
) WITH
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='NONE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'SizeTieredCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

```
## Configuration

TBD

## Thanks

Thanks go to Pierre-Yves Ritschard [https://github.com/pyr](https://github.com/pyr), Bruno Renié ([https://github.com/brutasse](https://github.com/brutasse)) - 
this project is useless without their work on **cyanite**, **graphite-api**, **graphite-cyanite**

## License

The MIT License (MIT)

Copyright (C) 2015 Andrei Ivanov

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.