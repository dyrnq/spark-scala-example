# spark-scala-example

This project is a proof of concept (POC) of a Spark Scala with s3-storage(minio) without HDFS or YARN example.

Pre-preparation

1. launch a spark standalone cluster.
2. launch a minio cluster for s3 storage.
3. create s3 access_key and secret_key
4. create s3 bucket 

build and deploy

`use build.sh`

```bash
build.sh -C "sample.DataProcessExample"
build.sh -C "sample.SparkPi"
build.sh -C "sample.WordCount"
```


```bash
    [spark standalone cluster] ---------read/write----------->> [minio-cluster]                                
                               ---------read/write----------->> [kafka-cluster] 
                               ---------read/write----------->> [jdbc]
                               ---------read/write----------->> [redis]
```

## ref

- <https://allaboutscala.com/big-data/spark/>
- <https://sparkbyexamples.com/>