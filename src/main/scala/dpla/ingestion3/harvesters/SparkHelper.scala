package dpla.ingestion3.harvesters

import com.amazonaws.auth.{AWSCredentialsProviderChain, DefaultAWSCredentialsProviderChain}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.Try


object SparkHelper {

  private val log: Logger = Logger.getLogger(SparkHelper.getClass)

  def spark(sparkConf: SparkConf) = {

    try {
      val awsCredentials = new AWSCredentialsProviderChain(new DefaultAWSCredentialsProviderChain).getCredentials
      val s3AccessKey: String = awsCredentials.getAWSAccessKeyId
      val s3SecretKey: String = awsCredentials.getAWSSecretKey
      sparkConf.set("spark.hadoop.fs.s3a.access.key", s3AccessKey)
      sparkConf.set("spark.hadoop.fs.s3a.secret.key", s3SecretKey)
    } catch {
      case e: Exception => log.info("S3 credentials not found. Not setting up S3 support.")
    }

    SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()
  }


}
