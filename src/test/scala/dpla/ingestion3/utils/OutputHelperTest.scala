package dpla.ingestion3.utils

import java.time.LocalDateTime

import org.scalatest.FlatSpec

class OutputHelperTest extends FlatSpec {

  val root = "s3a://my-bucket"
  val shortName = "foo"
  val activity = "harvest"
  val dateTime = LocalDateTime.of(2018, 9, 10, 9, 57, 2)

  val outputHelper = new OutputHelper(root, shortName, activity, dateTime)

  it should "throw exception if s3 protocol is tried" in {
    assertThrows[IllegalArgumentException](
      new OutputHelper("s3://my-bucket", shortName, activity, dateTime)
    )
  }

  it should "throw exception if s3n protocol is tried" in {
    assertThrows[IllegalArgumentException](
      new OutputHelper("s3n://my-bucket", shortName, activity, dateTime)
    )
  }

  it should "throw exception if activity not recognized" in {
    assertThrows[IllegalArgumentException](
      new OutputHelper(root, shortName, "oops", dateTime)
    )
  }

  "outputPath" should "create correct harvest output path" in {
    val path = "s3a://my-bucket/foo/harvest/20180910_095702-foo-OriginalRecord.avro"
    assert(outputHelper.outputPath === path)
  }

  "outputPath" should "create correct mapping output path" in {
    val helper = new OutputHelper(root, shortName, "mapping", dateTime)
    val path = "s3a://my-bucket/foo/mapping/20180910_095702-foo-MAP4_0.MAPRecord.avro"
    assert(helper.outputPath === path)
  }

  "outputPath" should "create correct enrichment output path" in {
    val helper = new OutputHelper(root, shortName, "enrichment", dateTime)
    val path = "s3a://my-bucket/foo/enrichment/20180910_095702-foo-MAP4_0.EnrichRecord.avro"
    assert(helper.outputPath === path)
  }

  "outputPath" should "create correct jsonl output path" in {
    val helper = new OutputHelper(root, shortName, "jsonl", dateTime)
    val path = "s3a://my-bucket/foo/jsonl/20180910_095702-foo-MAP3_1.IndexRecord.jsonl"
    assert(helper.outputPath === path)
  }

  "outputPath" should "create correct local output path" in {
    val localRoot = "/path/to/local"
    val helper = new OutputHelper(localRoot, shortName, activity, dateTime)
    val path = "/path/to/local/foo/harvest/20180910_095702-foo-OriginalRecord.avro"
    assert(helper.outputPath === path)
  }

  "bucketName" should "parse s3 bucket from given root" in {
    val bucket = "my-bucket"
    assert(outputHelper.bucketName === bucket)
  }

  "bucketNestedDir" should "parse s3 nested directory from given root" in {
    val nestedRoot = "s3a://foo/bar/bat/"
    val nestedDir = "bar/bat/"
    val helper = new OutputHelper(nestedRoot, shortName, activity, dateTime)
    assert(helper.bucketNestedDir === nestedDir)
  }

  "manifestKey" should "create correct manifest key" in {
    val key = "foo/harvest/20180910_095702-foo-OriginalRecord.avro/_MANIFEST"
    assert(outputHelper.manifestKey === key)
  }

  "manifestLocalOutPath" should "create correct local output path for manifest" in {
    val localRoot = "/path/to/local/"
    val helper = new OutputHelper(localRoot, shortName, activity, dateTime)
    val path = "/path/to/local/foo/harvest/20180910_095702-foo-OriginalRecord.avro/_MANIFEST"
    assert(helper.manifestLocalOutPath === path)
  }

  "logsBasePath" should "create correct base path for reports" in {
    val basePath = "s3a://my-bucket/foo/harvest/20180910_095702-foo-OriginalRecord.avro/_LOGS/"
    assert(outputHelper.logsBasePath === basePath)
  }
}
