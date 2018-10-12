package dpla.ingestion3.dataStorage

import org.scalatest._

class InputHelperTest extends FlatSpec {

  "isActivityPath" should "return true if given valid activity path" in {
    val path = "s3a://my-bucket/esdn/enrichment/20181003_215135-esdn-MAP4_0.EnrichRecord.avro/"
    assert(InputHelper.isActivityPath(path) == true)
  }

  "isActivityPath" should "return false if given invalid activity path" in {
    val path = "s3a://my-bucket/esdn/enrichment/"
    assert(InputHelper.isActivityPath(path) == false)
  }
}
