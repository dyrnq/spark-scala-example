# spark-scala-example

This project is a proof of concept (POC) for a Spark Scala application using S3 storage (MinIO)
without HDFS or YARN. It demonstrates integration with Iceberg, Hudi, Delta Lake, Kafka,
Gravitino, and Celeborn on a standalone Spark cluster.

## Cluster Environment

### Software Versions

| Component | Version | Notes |
|-----------|---------|-------|
| Spark (compile) | 3.5.8 | Standalone cluster, 3 masters |
| Spark (runtime) | 3.5.8 | `apache/spark:3.5.8-scala2.12-java17-python3-ubuntu` Docker image |
| Scala | 2.12.21 | |
| Java | 17 | From Docker image |
| Hadoop | 3.3.4 | AWS connector for S3A |
| Apache Iceberg | 1.11.0 | REST Catalog + S3FileIO |
| Apache Hudi | 1.2.0 | |
| Delta Lake | 3.3.2 | |
| Kafka Clients | 3.8.1 | |
| Apache Gravitino | 1.2.1 | Spark connector runtime |
| Apache Celeborn | 0.6.3 | Remote shuffle service (optional) |
| hutool | 5.8.39 | |
| Gson | 2.14.0 | |

### Infrastructure Nodes

| Node | Address | Role |
|------|---------|------|
| spark-master | `192.168.6.155:7077, 192.168.6.156:7077, 192.168.6.157:7077` | Spark standalone master (HA) |
| minio | `192.168.6.130:19000` | S3-compatible storage (bucket: `bigdata`) |
| kafka | `192.168.6.211:9092, 192.168.6.212:9092, 192.168.6.213:9092` | Kafka bootstrap servers |
| iceberg-rest | `192.168.6.152:9001` | Iceberg REST Catalog |
| iceberg-rest-multi | `192.168.6.159:29001` | Iceberg REST Catalog (multi-tenant) |
| gravitino | `192.168.6.159:8090` | Gravitino metastore |
| celeborn | `192.168.6.155:9097, 192.168.6.156:9097, 192.168.6.157:9097` | Celeborn shuffle master (optional) |
| http-jar-server | `192.168.6.171:3000` | HTTP server for distributing JAR files |

### Deployment Architecture

```
                                ┌─────────────────────────┐
                                │                         │
                                │   MinIO (S3) Cluster    │
                                │   192.168.6.130:19000   │
                                │                         │
                                └────────────┬────────────┘
                                             │
                                ┌────────────┴────────────┐
                                │                         │
                                │  Iceberg REST Catalog   │
                                │  192.168.6.152:9001     │
                                │  192.168.6.159:29001    │
                                │                         │
                                └────────────┬────────────┘
                                             │
 ┌─────────────────┐   ┌────────────────────┴────────────────────┐   ┌─────────────────────┐
 │                 │   │                                         │   │                     │
 │  Kafka Cluster  │◄──┤     Spark Standalone Cluster (3.5.8)    ├──►│  Gravitino Metastore│
 │  3 node         │   │     3 Masters + N Workers               │   │  192.168.6.159:8090 │
 │                 │   │                                         │   │                     │
 └─────────────────┘   └────────────┬────────────┬──────────────┘   └─────────────────────┘
                                     │            │
                                     │            │ (optional)
                                     │            │
                          ┌──────────┴──┐  ┌──────┴──────────┐
                          │ HTTP Server │  │ Celeborn Shuffle│
                          │ :3000 (JAR) │  │ 3 node          │
                          └─────────────┘  └─────────────────┘
```

Supported integrations:

- **Storage**: MinIO (S3A) — primary data lake storage
- **Table Formats**: Apache Iceberg (REST Catalog + Hadoop Catalog), Apache Hudi, Delta Lake
- **Streaming**: Kafka (structured streaming + DStream)
- **Metadata Federation**: Apache Gravitino (multi-catalog, multi-metalake)
- **Shuffle**: Apache Celeborn (remote shuffle service)

## Preparation

1. Start a Spark standalone cluster.
2. Start a MinIO cluster for S3 storage.
3. Create an S3 access key and secret key on MinIO.
4. Create an S3 bucket on MinIO.
5. Start a simple HTTP server with Python to share the target JAR files, for example: `python3 -m http.server --bind 0.0.0.0 3000`.

## Build and Deploy

Use `build.sh` to compile and submit:

```bash
./build.sh -C "sample.SparkPi"
./build.sh -C "sample.WordCount"
./build.sh -C "sample.DataProcessExample" -D cluster
./build.sh -C "sample.HudiSimple"
./build.sh -C "sample.HudiSQLSimple"
./build.sh -C "sample.IcebergSimple"
./build.sh -C "sample.IcebergRestSimple"
./build.sh -C "sample.IcebergRestMultiSupportSimple"
./build.sh -C "sample.DeltaSimple"
./build.sh -C "sample.GravitinoSparkSimple"
./build.sh -C "sample.OrdersAnalysis"
./build.sh -C "sample.OrdersAnalysisKafka"
./build.sh -C "sample.TestJarsExample"
```

### Build Script Options

| Option | Description | Default |
|--------|-------------|---------|
| `-C` / `--class` | Main class to submit (`sample.XXX`) | `sample.SparkPi` |
| `-D` / `--deploy-mode` | Deploy mode (`cluster` or `client`) | `cluster` |
| `--master` | Spark master URL | `spark://192.168.6.155:7077,192.168.6.156:7077,192.168.6.157:7077` |
| `-CS` / `--celeborn-shuffle` | Enable Celeborn remote shuffle | `false` |
| `--celeborn-master` | Celeborn master endpoints | `192.168.6.155:9097,192.168.6.156:9097,192.168.6.157:9097` |

### Dependency Distribution Strategy

In standalone cluster mode, `--packages` has [known limitations](https://github.com/dyrnq/spark-scala-example/issues/28)
with Ivy-based dependency resolution. This project uses a two-pronged approach:

- **Shaded uber-JAR** (`maven-shade-plugin`): All compile-scope dependencies are bundled into a
  single JAR (`spark-scala-example-1.0-SNAPSHOT-shaded.jar`), avoiding classpath conflicts.
- **`--jars` with HTTP URLs**: External dependencies not bundled in the uber-JAR (e.g. Hadoop AWS,
  Kafka, Hudi bundles, Gravitino connector) are distributed via direct HTTP URLs pointing to a
  Maven mirror (Huawei Cloud / Aliyun), allowing Spark to download them consistently on all
  driver and executor nodes.

## Sample Applications

### Core Examples

| Class | Description |
|-------|-------------|
| `SparkPi` | Classic Pi estimation with Spark |
| `WordCount` | Text word frequency count |
| `DataProcessExample` | Read CSV from S3 and join via Spark SQL |
| `DataFrameCreate` | DataFrame creation examples |
| `DataFrameForJoin` | DataFrame join operations |
| `FixCreateDF` | Fix for DataFrame creation |
| `RepartitionExample` | Repartition and coalesce examples |

### Table Format Examples

| Class | Description |
|-------|-------------|
| `DeltaSimple` | Delta Lake read/write with S3 |
| `HudiSimple` | Apache Hudi read/write with S3 |
| `HudiSQLSimple` | Apache Hudi via Spark SQL with HoodieCatalog |
| `IcebergSimple` | Apache Iceberg with Hadoop Catalog |
| `IcebergRestSimple` | Apache Iceberg with REST Catalog + S3FileIO |
| `IcebergRestMultiSupportSimple` | Multiple Iceberg REST Catalogs in one session |

### Streaming Examples

| Class | Description |
|-------|-------------|
| `OrdersAnalysis` | Order data analysis |
| `OrdersAnalysisKafka` | Streaming order analysis with Kafka source |

### Advanced Examples

| Class | Description |
|-------|-------------|
| `GravitinoSparkSimple` | Gravitino Spark connector for multi-catalog federation |
| `CustomCredentialProvider` | Custom AWS credentials provider for S3 |
| `TestJarsExample` | Testing `--jars` dependency loading |
| `GenerateOrdersCSV` | Generate sample orders CSV data (in `gen/`) |

## References

- [All About Scala - Big Data / Spark](https://allaboutscala.com/big-data/spark/)
- [Spark By Examples](https://sparkbyexamples.com/)
- [spark-dataframe-demo](https://github.com/parmarsachin/spark-dataframe-demo)
