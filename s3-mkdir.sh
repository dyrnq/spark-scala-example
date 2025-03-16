#!/usr/bin/env bash

conf_file="conf/spark-defaults.conf"

s3_endpoint=$(grep "^spark.hadoop.fs.s3a.endpoint=" "${conf_file}" | cut -d '=' -f 2-)
s3_access_key=$(grep "^spark.hadoop.fs.s3a.access.key=" "${conf_file}" | cut -d '=' -f 2-)
s3_secret_key=$(grep "^spark.hadoop.fs.s3a.secret.key=" "${conf_file}" | cut -d '=' -f 2-)
s3_bucket="bigdata"


docker run -i --rm --network host --entrypoint '' minio/mc bash<<EOF
mc alias set myminio http://${s3_endpoint} ${s3_access_key} ${s3_secret_key};
mc mb myminio/${s3_bucket} 2>/dev/null;
EOF

docker run -i --rm --network host --entrypoint '' minio/mc bash<<EOF
mc alias set myminio http://${s3_endpoint} ${s3_access_key} ${s3_secret_key};
echo "" | mc pipe myminio/${s3_bucket}/_SUCCESS
echo "" | mc pipe myminio/${s3_bucket}/iceberg/_SUCCESS
echo "" | mc pipe myminio/${s3_bucket}/spark-logs/_SUCCESS
echo "" | mc pipe myminio/${s3_bucket}/spark-checkpoints/orders/_SUCCESS
echo "" | mc pipe myminio/${s3_bucket}/delta/_SUCCESS

EOF


