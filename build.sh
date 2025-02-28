#!/usr/bin/env bash

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd -P)
echo "SCRIPT_DIR=${SCRIPT_DIR}"
class=${class:-sample.SparkPi}
while [ $# -gt 0 ]; do
    case "$1" in
        --class|-C)
            class="$2"
            shift
            ;;
        --*)
            echo "Illegal option $1"
            ;;
    esac
    shift $(( $# > 0 ? 1 : 0 ))
done


local_maven_repo=$(mvn help:evaluate -Dexpression=settings.localRepository |grep -v "INFO")
#local_maven_repo=/data/maven/repository
echo "local_maven_repo=${local_maven_repo}"

spark_image="apache/spark:3.5.4-scala2.12-java11-python3-ubuntu"
spark_master="spark://spark-master-1:7077,spark-master-2:7077,spark-master-3:7077"
hadoop_ver=3.3.4
aws_ver=1.12.367
aws2_ver=2.30.27

#mvn dependency:get -Dartifact=software.amazon.awssdk:bundle:${aws2_ver}
mvn dependency:get -Dartifact=org.apache.hadoop:hadoop-aws:${hadoop_ver}
mvn dependency:get -Dartifact=com.amazonaws:aws-java-sdk-bundle:${aws_ver}
# https://stackoverflow.com/questions/39906536/spark-history-server-on-s3a-filesystem-classnotfoundexception/65086818#65086818


dep_jars="${local_maven_repo}/org/apache/hadoop/hadoop-aws/${hadoop_ver}/hadoop-aws-${hadoop_ver}.jar,${local_maven_repo}/com/amazonaws/aws-java-sdk-bundle/${aws_ver}/aws-java-sdk-bundle-${aws_ver}.jar"
echo "dep_jars=${dep_jars}"

mvn clean package

docker run \
-it \
--rm \
--network=canal \
-v ./target:/target \
-v ${local_maven_repo}:${local_maven_repo} \
-v /data/work/club/poc/spark/conf/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf \
"${spark_image}" \
/opt/spark/bin/spark-submit \
--class "${class}" \
--deploy-mode cluster \
--master "${spark_master}" \
--jars "${dep_jars}" \
http://192.168.6.171:3000/target/spark-scala-example-1.0-SNAPSHOT.jar


