# spark-scala-example

This project is a proof of concept (POC) of a Spark Scala example.

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

## ref

- <https://allaboutscala.com/big-data/spark/>
- <https://sparkbyexamples.com/>