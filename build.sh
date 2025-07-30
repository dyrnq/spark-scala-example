#!/usr/bin/env bash

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd -P)
echo "SCRIPT_DIR=${SCRIPT_DIR}"
class=${class:-sample.SparkPi}
deploy_mode=${deploy_mode:-cluster}
#spark_master="spark://spark-master-1:7077,spark-master-2:7077,spark-master-3:7077"
spark_master="spark://192.168.6.155:7077,192.168.6.156:7077,192.168.6.157:7077"
#spark_image="apache/spark:3.5.4-scala2.12-java17-python3-ubuntu"
#spark_image="bitnami/spark:3.5.4-debian-12-r6"

#spark_image="apache/spark:3.5.6-scala2.12-java17-python3-ubuntu"
spark_image="bitnami/spark:3.5.6-debian-12-r1"
spark_home=""

shuffle_celeborn="false"
celeborn_master="192.168.6.155:9097,192.168.6.156:9097,192.168.6.157:9097"


if grep -q bitnami <<< "${spark_image}" ; then
  spark_home="/opt/bitnami/spark"
else
  spark_home="/opt/spark"
fi

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
        --celeborn-shuffle|-CS)
            shuffle_celeborn="$2"
            shift
            ;;
        --celeborn-master)
            celeborn_master="$2"
            shift
            ;;
        --*)
            echo "Illegal option $1"
            ;;
    esac
    shift $(( $# > 0 ? 1 : 0 ))
done


#mvn(){
#  "${MAVEN_HOME}"/bin/mvn $@ -s settings.xml
#}
# https://stackoverflow.com/questions/5916157/how-to-get-the-maven-local-repo-location/5916233




local_maven_repo=$(mvn help:evaluate -Dexpression=settings.localRepository -q -DforceStdout)
local_maven_repo="/opt/maven-repo"
# local_maven_repo=$(mvn help:evaluate -Dartifact=org.apache.commons:commons-lang3:3.17.0 -Dexpression=settings.localRepository |grep -v -E "^Downloading" |grep -v "INFO" |grep -v "WARNING" | head -n1)

# local_maven_repo=/data/maven/repository
echo "local_maven_repo=${local_maven_repo}"



#mvn dependency:get -Dartifact=software.amazon.awssdk:bundle:2.30.27
#mvn dependency:get -Dartifact=org.apache.hadoop:hadoop-aws:3.3.4
#mvn dependency:get -Dartifact=com.amazonaws:aws-java-sdk-bundle:1.12.367
#mvn dependency:get -Dartifact=org.apache.kafka:kafka-clients:3.8.1
#mvn dependency:get -Dartifact=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4
#mvn dependency:get -Dartifact=org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.4
#mvn dependency:get -Dartifact=org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.4
#mvn dependency:get -Dartifact=org.apache.commons:commons-pool2:2.12.1
#mvn dependency:get -Dartifact=org.apache.hudi:hudi-utilities-bundle_2.12:1.0.1
#mvn dependency:get -Dartifact=org.apache.spark:spark-connect_2.12:3.5.4
#mvn dependency:get -Dartifact=org.apache.celeborn:celeborn-client-spark-3-shaded_2.12:0.6.0
# https://stackoverflow.com/questions/39906536/spark-history-server-on-s3a-filesystem-classnotfoundexception/65086818#65086818

PACKAGES=$(cat <<EOF | tr '\n' ',' | sed 's/,$//'
org.apache.hadoop:hadoop-aws:3.3.4
com.amazonaws:aws-java-sdk-bundle:1.12.367
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4
org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.4
org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.4
org.apache.kafka:kafka-clients:3.8.1
org.apache.commons:commons-pool2:2.12.1
org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.1
org.apache.hudi:hudi-utilities-bundle_2.12:1.0.1
cn.hutool:hutool-all:5.8.39
org.apache.celeborn:celeborn-client-spark-3-shaded_2.12:0.6.0
EOF
)



LOCAL_PACKAGES="";
IFS=',' read -ra JARS <<< "$PACKAGES"
for jar in "${JARS[@]}"; do
GROUP_ID="$(echo $jar | cut -d: -f1)"
ARTIFACT_ID="$(echo $jar | cut -d: -f2)"
VERSION="$(echo $jar | cut -d: -f3)"
LOCAL_PACKAGES="${LOCAL_PACKAGES}${ARTIFACT_ID}-${VERSION}.jar:"
done

LOCAL_PACKAGES=$(echo "${LOCAL_PACKAGES}" | sed 's/:$//') ## 删除最后一个,号


MAVEN_PATH_PACKAGES="";
IFS=',' read -ra JARS <<< "$PACKAGES"
for jar in "${JARS[@]}"; do
GROUP_ID="$(echo $jar | cut -d: -f1)"
ARTIFACT_ID="$(echo $jar | cut -d: -f2)"
VERSION="$(echo $jar | cut -d: -f3)"
JAR_WITHPATH="${local_maven_repo}/${GROUP_ID//./\/}/${ARTIFACT_ID}/${VERSION}/${ARTIFACT_ID}-${VERSION}.jar"
if [ ! -e "${JAR_WITHPATH}" ]; then
    mvn dependency:get -Dartifact=${jar} -Dmaven.repo.local=${local_maven_repo}
fi
MAVEN_PATH_PACKAGES="${MAVEN_PATH_PACKAGES}${JAR_WITHPATH}:"
done

MAVEN_PATH_PACKAGES=$(echo "${MAVEN_PATH_PACKAGES}" | sed 's/:$//') ## 删除最后一个,号



#mvn clean package -Dmaven.test.skip=true --threads 2C


s3_access_key=$(grep spark.hadoop.fs.s3a.access.key "${SCRIPT_DIR}"/conf/spark-defaults.conf | awk -F"=" '{ print $2}')
s3_secret_key=$(grep spark.hadoop.fs.s3a.secret.key "${SCRIPT_DIR}"/conf/spark-defaults.conf | awk -F"=" '{ print $2}')

echo "s3_access_key=${s3_access_key}"
echo "s3_secret_key=${s3_secret_key}"

#-e AWS_REGION="us-east-1" \
#-e AWS_ACCESS_KEY_ID="${s3_access_key}" \
#-e AWS_SECRET_ACCESS_KEY="${s3_secret_key}" \
# -Daws.region=us-east-1 -Daws.accessKeyId="${s3_access_key}" -Daws.secretAccessKey="${s3_secret_key}"



IVY_PACKAGES="";
IFS=',' read -ra JARS <<< "$PACKAGES"
for jar in "${JARS[@]}"; do
GROUP_ID="$(echo $jar | cut -d: -f1)"
ARTIFACT_ID="$(echo $jar | cut -d: -f2)"
VERSION="$(echo $jar | cut -d: -f3)"
IVY_PACKAGES="${IVY_PACKAGES}${spark_home}/.ivy/jars/${GROUP_ID}_${ARTIFACT_ID}-${VERSION}.jar:"
done

IVY_PACKAGES=$(echo "${IVY_PACKAGES}" | sed 's/:$//') ## 删除最后一个,号






dynamic_conf=""
if [ "${shuffle_celeborn}" = "true" ]; then

dynamic_conf=$(cat <<EOF | tr '\n' ' ' | sed 's/,$//'
--conf spark.shuffle.manager=org.apache.spark.shuffle.celeborn.SparkShuffleManager
--conf spark.shuffle.service.enabled=false
--conf spark.celeborn.master.endpoints=${celeborn_master}
EOF
)

fi

java_opts=$(cat <<EOF | grep -v '^\s*#' | tr '\n' ' ' | sed 's/,$//'
#-Daws.region=us-east-1
#-Daws.accessKeyId="${s3_access_key}"
#-Daws.secretAccessKey="${s3_secret_key}"
#--add-exports java.base/sun.net.util=ALL-UNNAMED
#--add-exports java.rmi/sun.rmi.registry=ALL-UNNAMED
#--add-exports jdk.compiler/com.sun.tools.javac.api=ALL-UNNAMED
#--add-exports jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED
#--add-exports jdk.compiler/com.sun.tools.javac.parser=ALL-UNNAMED
#--add-exports jdk.compiler/com.sun.tools.javac.tree=ALL-UNNAMED
#--add-exports jdk.compiler/com.sun.tools.javac.util=ALL-UNNAMED
#--add-exports java.security.jgss/sun.security.krb5=ALL-UNNAMED
-Djava.net.preferIPv6Addresses=false
-XX:+IgnoreUnrecognizedVMOptions
-Djdk.reflect.useDirectMethodHandle=false
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED
--add-opens=java.base/java.io=ALL-UNNAMED
--add-opens=java.base/java.net=ALL-UNNAMED
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED
--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
--add-opens=java.base/sun.nio.cs=ALL-UNNAMED
--add-opens=java.base/sun.security.action=ALL-UNNAMED
--add-opens=java.base/sun.util.calendar=ALL-UNNAMED
--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED
--add-opens=java.base/java.text=ALL-UNNAMED
--add-opens=java.base/java.time=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED
--add-opens=java.base/java.math=ALL-UNNAMED
--add-opens=java.base/java.security=ALL-UNNAMED
--add-opens=java.base/jdk.internal.access=ALL-UNNAMED
--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED
--add-opens=java.base/sun.net.util=ALL-UNNAMED
EOF
)

echo "${dynamic_conf}"
echo "${java_opts}"
echo "${MAVEN_PATH_PACKAGES}"
echo "${LOCAL_PACKAGES}"
#--conf "spark.jars.ivySettings=${spark_home}/conf/ivysettings.xml" \

set -x;
docker run \
-it \
--rm \
--network=host \
-v ./target:/target \
-v ./conf/spark-defaults.conf:${spark_home}/conf/spark-defaults.conf \
-v ./conf/ivysettings.xml:${spark_home}/conf/ivysettings.xml \
-v "${local_maven_repo}":"${local_maven_repo}" \
"${spark_image}" \
${spark_home}/bin/spark-submit \
--packages "${PACKAGES}" \
--files "${spark_home}/conf/spark-defaults.conf" \
--class "${class}" \
--deploy-mode "${deploy_mode}" \
--master "${spark_master}" \
--conf spark.jars.ivy=${spark_home}/.ivy \
--conf spark.jars.ivySettings=${spark_home}/conf/ivysettings.xml \
--conf spark.driver.extraJavaOptions="${java_opts}" \
--conf spark.executor.extraJavaOptions="${java_opts}" \
--conf spark.driver.extraClassPath="${IVY_PACKAGES}" \
--conf spark.executor.extraClassPath="${IVY_PACKAGES}" \
${dynamic_conf} \
http://192.168.6.171:3000/target/spark-scala-example-1.0-SNAPSHOT-shaded.jar

#--conf "spark.driver.extraClassPath=${MAVEN_PATH_PACKAGES}" \
#--conf "spark.executor.extraClassPath=${MAVEN_PATH_PACKAGES}" \
#--conf "spark.jars.packages=${PACKAGES}" \
#--conf "spark.driver.extraClassPath=${LOCAL_PACKAGES}" \
#--conf "spark.executor.extraClassPath=${LOCAL_PACKAGES}" \

# --jars "${MAVEN_PATH_PACKAGES}" \
# --packages "${PACKAGES}" \
#--conf "spark.driver.extraJavaOptions=${extraJavaOptions}" \
#--conf "spark.executor.extraJavaOptions=${extraJavaOptions}" \
#spark.driver.extraClassPath=/home/mahesh.gupta/hudi-utilities-bundle_2.12-0.14.1.jar
#spark.executor.extraClassPath=/home/mahesh.gupta/hudi-utilities-bundle_2.12-0.14.1.jar

