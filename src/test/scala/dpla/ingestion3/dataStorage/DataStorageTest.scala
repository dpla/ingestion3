package dpla.ingestion3.dataStorage

import org.scalatest._

class DataStorageTest extends FlatSpec {

  "parseS3Address" should "correctly parse an S3 protocol" in {
    val path = "s3a://my-bucket/foo/bar/"
    assert(parseS3Address(path).protocol == "s3a")
  }

  "parseS3Address" should "correctly parse an S3 bucket" in {
    val path = "s3a://my-bucket/foo/bar/"
    assert(parseS3Address(path).bucket == "my-bucket")
  }

  "parseS3Address" should "correctly parse an S3 prefix" in {
    val path = "s3a://my-bucket/foo/bar/"
    assert(parseS3Address(path).prefix == Some("foo/bar"))
  }

  "parseS3Address" should "throw exception in absence of s3 protocol" in {
    val badPath = "/foo/bar"
    assertThrows[RuntimeException](parseS3Address(badPath))
  }

  "parseS3Address" should "throw exception in absence of bucket" in {
    val badPath = "s3a://"
    assertThrows[RuntimeException](parseS3Address(badPath))
  }

  "S3Address.fullPath" should "compose full path" in {
    val address = S3Address("s3a", "my-bucket", Some("prefix"))
    assert(S3Address.fullPath(address) == "s3a://my-bucket/prefix")
  }

  "S3Address.fullPath" should "handle leading/trailing slashes" in {
    val address = S3Address("s3a", "my-bucket/", Some("/prefix/"))
    assert(S3Address.fullPath(address) == "s3a://my-bucket/prefix")
  }
}
