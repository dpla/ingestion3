package la.dp.ingestion3.harvesters

import java.io.File
import java.net.URL

import org.scalatest._


/**
  * Tests for OaiHarvester
  *
  * TODO: Figure out how to simulate http requests/responses.
  *
  */
class OaiHarvesterTest extends FlatSpec with Matchers {
  val file = new File("/dev/null")
  val oaiUrl = new java.net.URL("http://aggregator.padigital.org/oai")
  val oaiVerb = "ListRecords"
  val prefix = "oai_dc"
  val harvester = new OaiHarvester(endpoint = oaiUrl,
                                   metadataPrefix = prefix,
                                   outDir = file)

  val validOaiXml = la.dp.ingestion3.data.TestOaiData.pa_oai
  val invalidOaiXml = la.dp.ingestion3.data.TestOaiData.pa_error

  "buildQueryUrl (when not given a resumptionToken) " should " return a URL" in {
    val queryUrl = harvester.buildQueryUrl(verb="ListRecords")
    assert(queryUrl.isInstanceOf[URL])
  }

  "buildQueryUrl (when given a resumptionToken) " should " return a URL" in {
    val queryUrl = harvester.buildQueryUrl(resumptionToken = "pickup1", verb="ListRecords")
    assert(queryUrl.isInstanceOf[URL])
  }

  "checkOaiErrorCode " should " throw a HarvesterException checking 'badArgument'" in {
    assertThrows[HarvesterException] { harvester.checkOaiErrorCode("badArgument") }
  }

  it should "throw a HarvesterException checking 'badResumptionToken" in {
    assertThrows[HarvesterException] { harvester.checkOaiErrorCode("badResumptionToken") }
  }

  it should "throw a HarvesterException checking 'badVerb" in {
    assertThrows[HarvesterException] { harvester.checkOaiErrorCode("badVerb") }
  }

  it should "throw a HarvesterException checking 'idDoesNotExist" in {
    assertThrows[HarvesterException] { harvester.checkOaiErrorCode("idDoesNotExist") }
  }

  it should "throw a HarvesterException checking 'noMetadataFormats" in {
    assertThrows[HarvesterException] { harvester.checkOaiErrorCode("noMetadataFormats") }
  }

  it should "throw a HarvesterException checking 'noSetHierarchy" in {
    assertThrows[HarvesterException] { harvester.checkOaiErrorCode("noSetHierarchy") }
  }

  it should "throw a HarvesterException checking 'cannotDisseminateFormat" in {
    assertThrows[HarvesterException] { harvester.checkOaiErrorCode("cannotDisseminateFormat") }
  }

  it should "throw a HarvesterException checking 'non-Spec complient error code" in {
    assertThrows[HarvesterException] { harvester.checkOaiErrorCode("non-Spec complient error message") }
  }

  it should " return nothing if there is no error code" in {
    harvester.checkOaiErrorCode("")
  }

  "getOaiErrorCode " should " return an empty string if there is no error code" in {
      assert(harvester.getOaiErrorCode(validOaiXml).isEmpty)
  }

  it should " return a non-Empty string if there is an error code " in {
    val errorCode = harvester.getOaiErrorCode(invalidOaiXml)
    assert(errorCode.nonEmpty)
  }

  "getHarvestedRecords " should " return a map of size 10 from data.pa_oai" in {
    assert(harvester.getHarvestedRecords(validOaiXml).size == 10)
  }

  "getResumptionToken() " should " return an empty String if there is an error in the response " in {
    assert(harvester.getResumptionToken(invalidOaiXml).isEmpty)
  }

  it should " return a non-empty String value if the response is valid and incomplete" in {
    assert(harvester.getResumptionToken(validOaiXml).nonEmpty)
  }
  it should " equal '90d421891feba6922f57a59868d7bcd1'" in {
    assert(harvester.getResumptionToken(validOaiXml) == "90d421891feba6922f57a59868d7bcd1")
  }

  "getXmlResponse (when given an invalid URL) " should " throw a HarvesterException exception" in {
    val badUrl = new java.net.URL("http://aggregator.dev.fake/oai")
    val badHarvester = new OaiHarvester( endpoint = badUrl,
                                          metadataPrefix = prefix,
                                          outDir = file)

    val q_url = badHarvester.buildQueryUrl(resumptionToken = "", verb = oaiVerb)
    assertThrows[HarvesterException] { badHarvester.getXmlResponse(q_url) }
  }

  /**
    * TODO Figure out mocks...
    */
  it should " throw a SaxParserException when given bad XML" in {
    // harvester.getXmlResponse()
  }
}
