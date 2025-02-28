# spark-scala-example

This project is a proof of concept (POC) for a Spark Scala application using S3 storage (Minio) without HDFS or YARN.

Preparation

1. Start a Spark standalone cluster.
2. Start a Minio cluster for S3 storage.
3. Create an S3 access key and secret key on Minio.
4. Create an S3 bucket on Minio.
5. Start a simple HTTP server with Python to share the target JAR files, for example: python3 -m http.server --bind 0.0.0.0 3000.

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