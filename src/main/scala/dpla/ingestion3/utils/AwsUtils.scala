package dpla.ingestion3.utils

import com.amazonaws.auth.{AWSCredentialsProviderChain, DefaultAWSCredentialsProviderChain}

/**
  * Gets AWS access keys
  */
trait AwsUtils {

  protected lazy val s3AccessKey: String = awsCredentials.getCredentials.getAWSAccessKeyId
  protected lazy val s3SecretKey: String = awsCredentials.getCredentials.getAWSSecretKey


  /**
    * DefaultAWSCredentialsProviderChain looks for AWS keys in the following order:
    *   1. Environment Variables
    *   2. Java System Properties
    *   3. Credential profiles file at the default location (~/.aws/credentials)
    *   4. Instance profile credentials delivered through the Amazon EC2 metadata service
    *
    * @return
    */
  private val awsCredentials = new AWSCredentialsProviderChain(new DefaultAWSCredentialsProviderChain)
}
