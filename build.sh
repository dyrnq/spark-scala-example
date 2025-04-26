#!/usr/bin/env bash

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd -P)
echo "SCRIPT_DIR=${SCRIPT_DIR}"
class=${class:-sample.SparkPi}
deploy_mode=${deploy_mode:-cluster}
spark_master="spark://spark-master-1:7077,spark-master-2:7077,spark-master-3:7077"
spark_image="apache/spark:3.5.4-scala2.12-java11-python3-ubuntu"
hadoop_ver=3.3.4
aws_ver=1.12.367
aws2_ver=2.30.27

while [ $# -gt 0 ]; do
    case "$1" in
        --class|-C)
            class="$2"
            shift
            ;;
        --deploy-mode|-D)
            deploy_mode="$2"
            shift
            ;;
        --master)
            spark_master="$2"
            shift
            ;;
        --*)
            echo "Illegal option $1"
            ;;
    esac
    shift $(( $# > 0 ? 1 : 0 ))
done


local_maven_repo=$(mvn help:evaluate -Dexpression=settings.localRepository |grep -v "INFO" |grep -v "WARNING" | head -n1)

#local_maven_repo=/data/maven/repository
echo "local_maven_repo=${local_maven_repo}"



#mvn dependency:get -Dartifact=software.amazon.awssdk:bundle:${aws2_ver}
mvn dependency:get -Dartifact=org.apache.hadoop:hadoop-aws:${hadoop_ver}
mvn dependency:get -Dartifact=com.amazonaws:aws-java-sdk-bundle:${aws_ver}
mvn dependency:get -Dartifact=org.apache.kafka:kafka-clients:3.8.1
mvn dependency:get -Dartifact=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4
mvn dependency:get -Dartifact=org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.4
mvn dependency:get -Dartifact=org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.4
mvn dependency:get -Dartifact=org.apache.commons:commons-pool2:2.12.1
mvn dependency:get -Dartifact=org.apache.hudi:hudi-utilities-bundle_2.12:1.0.1
# https://stackoverflow.com/questions/39906536/spark-history-server-on-s3a-filesystem-classnotfoundexception/65086818#65086818



dep_jars="${local_maven_repo}/org/apache/hadoop/hadoop-aws/${hadoop_ver}/hadoop-aws-${hadoop_ver}.jar"
dep_jars="${dep_jars},${local_maven_repo}/com/amazonaws/aws-java-sdk-bundle/${aws_ver}/aws-java-sdk-bundle-${aws_ver}.jar"
dep_jars="${dep_jars},${local_maven_repo}/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.4/spark-sql-kafka-0-10_2.12-3.5.4.jar"
dep_jars="${dep_jars},${local_maven_repo}/org/apache/spark/spark-streaming-kafka-0-10_2.12/3.5.4/spark-streaming-kafka-0-10_2.12-3.5.4.jar"
dep_jars="${dep_jars},${local_maven_repo}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.4/spark-token-provider-kafka-0-10_2.12-3.5.4.jar"
dep_jars="${dep_jars},${local_maven_repo}/org/apache/kafka/kafka-clients/3.8.1/kafka-clients-3.8.1.jar"
dep_jars="${dep_jars},${local_maven_repo}/org/apache/commons/commons-pool2/2.12.1/commons-pool2-2.12.1.jar"
echo "dep_jars=${dep_jars}"
dep_jars="${local_maven_repo}/org/apache/hudi/hudi-utilities-bundle_2.12/1.0.1/hudi-utilities-bundle_2.12-1.0.1.jar"
echo "dep_jars=${dep_jars}"
mvn clean package


s3_access_key=$(grep spark.hadoop.fs.s3a.access.key "${SCRIPT_DIR}"/conf/spark-defaults.conf | awk -F"=" '{ print $2}')
s3_secret_key=$(grep spark.hadoop.fs.s3a.secret.key "${SCRIPT_DIR}"/conf/spark-defaults.conf | awk -F"=" '{ print $2}')

echo "s3_access_key=${s3_access_key}"
echo "s3_secret_key=${s3_secret_key}"


set -x;
docker run \
-it \
--rm \
--network=canal \
-e AWS_REGION="us-east-1" \
-e AWS_ACCESS_KEY_ID="${s3_access_key}" \
-e AWS_SECRET_ACCESS_KEY="${s3_secret_key}" \
-v ./target:/target \
-v /data/work/club/poc/spark/conf/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf \
"${spark_image}" \
/opt/spark/bin/spark-submit \
--class "${class}" \
--deploy-mode "${deploy_mode}" \
--master "${spark_master}" \
--conf "spark.driver.extraClassPath=${dep_jars}" \
--conf "spark.executor.extraClassPath=${dep_jars}" \
http://192.168.6.171:3000/target/spark-scala-example-1.0-SNAPSHOT-shaded.jar

#spark.driver.extraClassPath=/home/mahesh.gupta/hudi-utilities-bundle_2.12-0.14.1.jar
#spark.executor.extraClassPath=/home/mahesh.gupta/hudi-utilities-bundle_2.12-0.14.1.jar

