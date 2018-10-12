package dpla.ingestion3.dataStorage

import java.time.LocalDateTime

import org.scalatest.{FlatSpec, Matchers}

class OutputHelperTest extends FlatSpec with Matchers {

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

  it should "throw exception if activity is not recognized" in {
    assertThrows[IllegalArgumentException](
      new OutputHelper(root, shortName, "foo", dateTime)
    )
  }

  "s3address" should "recognize s3 path" in {
    outputHelper.s3Address should not be empty
  }

  "s3address" should "recognize non-s3 path" in {
    val localRoot = "/path/to/local"
    val helper = new OutputHelper(localRoot, shortName, activity, dateTime)
    helper.s3Address shouldBe empty
  }

  "activityPath" should "create correct harvest output path" in {
    val path = "s3a://my-bucket/foo/harvest/20180910_095702-foo-OriginalRecord.avro"
    assert(outputHelper.activityPath === path)
  }

  "activityPath" should "create correct mapping output path" in {
    val helper = new OutputHelper(root, shortName, "mapping", dateTime)
    val path = "s3a://my-bucket/foo/mapping/20180910_095702-foo-MAP4_0.MAPRecord.avro"
    assert(helper.activityPath === path)
  }

  "activityPath" should "create correct enrichment output path" in {
    val helper = new OutputHelper(root, shortName, "enrichment", dateTime)
    val path = "s3a://my-bucket/foo/enrichment/20180910_095702-foo-MAP4_0.EnrichRecord.avro"
    assert(helper.activityPath === path)
  }

  "activityPath" should "create correct jsonl output path" in {
    val helper = new OutputHelper(root, shortName, "jsonl", dateTime)
    val path = "s3a://my-bucket/foo/jsonl/20180910_095702-foo-MAP3_1.IndexRecord.jsonl"
    assert(helper.activityPath === path)
  }

  "activityPath" should "create correct reports output path" in {
    val helper = new OutputHelper(root, shortName, "reports", dateTime)
    val path = "s3a://my-bucket/foo/reports/20180910_095702-foo-reports"
    assert(helper.activityPath === path)
  }

  "activityPath" should "create correct local output path" in {
    val localRoot = "/path/to/local"
    val helper = new OutputHelper(localRoot, shortName, activity, dateTime)
    val path = "/path/to/local/foo/harvest/20180910_095702-foo-OriginalRecord.avro"
    assert(helper.activityPath === path)
  }

  "manifestPath" should "create correct S3 output path" in {
    val path = "s3a://my-bucket/foo/harvest/20180910_095702-foo-OriginalRecord.avro/_MANIFEST"
    assert(outputHelper.manifestPath === path)
  }

  "manifestPath" should "create correct local output path" in {
    val localRoot = "/path/to/local/"
    val helper = new OutputHelper(localRoot, shortName, activity, dateTime)
    val path = "/path/to/local/foo/harvest/20180910_095702-foo-OriginalRecord.avro/_MANIFEST"
    assert(helper.manifestPath === path)
  }

  "logsPath" should "create correct S3 output path" in {
    val path = "s3a://my-bucket/foo/harvest/20180910_095702-foo-OriginalRecord.avro/_LOGS"
    assert(outputHelper.logsPath === path)
  }

  "logsPath" should "create correct local output path" in {
    val localRoot = "/path/to/local/"
    val helper = new OutputHelper(localRoot, shortName, activity, dateTime)
    val path = "/path/to/local/foo/harvest/20180910_095702-foo-OriginalRecord.avro/_LOGS"
    assert(helper.logsPath === path)
  }
}
