package la.dp.ingestion3.harvesters

import org.scalatest._


/**
  * Tests for OAIResponseTraversable
  *
  * TODO: Figure out how to simulate http requests/responses.
  *
  */
class OAIResponseTraversableTest extends FlatSpec with Matchers with BeforeAndAfter {

//  val validOaiXml = la.dp.ingestion3.data.TestOaiData.paOaiListRecordsRsp
//  val invalidOaiXml = la.dp.ingestion3.data.TestOaiData.paOaiErrorRsp
//
//  val outDir = new File("/dev/null")
//  val oaiUrl = new java.net.URL("http://aggregator.padigital.org/oai")
//  val oaiVerb = "ListRecords"
//  val prefix = "oai_dc"
//  val fileIO = new FlatFileIO
//  val queryUrlBuilder = new OaiQueryUrlBuilder
//  val harvester = new OAIResponseTraversable()
//
//  "getOaiErrorCode " should " return Option.None if there is no error code" in {
//    harvester.getOaiErrorCode(validOaiXml) shouldBe None
//  }
//
//  it should " throw a HarvesterException if there is an error code " in {
//    assertThrows[HarvesterException] {
//      harvester.getOaiErrorCode(invalidOaiXml)
//    }
//  }
//
//  "getResumptionToken() " should " return None if there is an error in the response " in {
//    assert(harvester.getResumptionToken(invalidOaiXml) === None)
//  }
//
//  it should " return a non-empty String value if the response is valid and incomplete" in {
//    assert(harvester.getResumptionToken(validOaiXml).get.nonEmpty)
//  }
//  it should " equal '90d421891feba6922f57a59868d7bcd1'" in {
//    assert(harvester.getResumptionToken(validOaiXml).get === "90d421891feba6922f57a59868d7bcd1")
//  }
}
