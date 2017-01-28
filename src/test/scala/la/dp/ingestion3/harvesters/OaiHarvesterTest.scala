package la.dp.ingestion3.harvesters

import java.io.File

import org.scalatest.{FlatSpec, Matchers}

/**
  * Tests for OaiHarvester
  *
  *   TODO: Figure out how to simulate http requests/responses.
  *
  */
class OaiHarvesterTest extends FlatSpec with Matchers {
  val file = new File("/dev/null")
  val oai_url = new java.net.URL("http://dev.com")
  val harvester = new OaiHarvester(oai_url, "ListRecords", file)


  "checkOaiErrorCode() " should " throw an HarvesterException if a badArgument error code is returned" in {
    val errorCode = "badArguement"
    assertThrows[Exception] {
      harvester.checkOaiErrorCode(errorCode)
    }
  }

  "getResumptionToken() " should " return an empty String if there is an error in the response " in {
    val xml = la.dp.ingestion3.data.TestOaiData.pa_error
    assert(harvester.getResumptionToken(xml) == "")
  }

  it should " return a non-empty String value if the response is valid and incomplete" in {
    val xml = la.dp.ingestion3.data.TestOaiData.pa_oai
    assert(harvester.getResumptionToken(xml).nonEmpty)
    assert(harvester.getResumptionToken(xml) == "90d421891feba6922f57a59868d7bcd1")
  }
}
