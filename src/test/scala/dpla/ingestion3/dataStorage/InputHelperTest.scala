package dpla.ingestion3.dataStorage

import org.scalatest.flatspec.AnyFlatSpec

class InputHelperTest extends AnyFlatSpec {

  "isActivityPath" should "return true if given valid activity path" in {
    val path = "s3a://my-bucket/esdn/enrichment/20181003_215135-esdn-MAP4_0.EnrichRecord.avro/"
    assert(InputHelper.isActivityPath(path))
  }

  "isActivityPath" should "return false if given invalid activity path" in {
    val path = "s3a://my-bucket/esdn/enrichment/"
    assert(!InputHelper.isActivityPath(path))
  }
}
