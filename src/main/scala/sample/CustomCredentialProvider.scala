package sample

import scala.jdk.CollectionConverters._

import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, AwsCredentials, AwsCredentialsProvider}

class CustomCredentialProvider private(
                                        private val credentials: AwsCredentials
                                      ) extends AwsCredentialsProvider {

  override def resolveCredentials: AwsCredentials = this.credentials
}

object CustomCredentialProvider {
  def create(keys: java.util.Map[String, String]): CustomCredentialProvider = {

    // 打印 keys 的键和值
//    for (key <- keys.keySet().asScala) {
//      println(s"Key: $key, Value: ${keys.get(key)}")
//    }

    // 检查是否包含必要的键
    require(
      keys.containsKey("accessKeyId") && keys.containsKey("secretAccessKey"),
      "keys must contain accessKeyId and secretAccessKey"
    )


    new CustomCredentialProvider(AwsBasicCredentials.create(keys.get("accessKeyId"), keys.get("secretAccessKey")))
  }

//  def create(): CustomCredentialProvider = {
//    create(Map.empty)
//  }
}



// https://github.com/apache/iceberg/issues/10078