package la.dp.ingestion3.harvesters

import java.io.File
import java.net.{URL, UnknownHostException}

import org.scalatest._

import scala.xml.NodeSeq

/**
  * Tests for OaiHarvester
  *
  * TODO: Figure out how to simulate http requests/responses.
  *
  */
class OaiHarvesterTest extends FlatSpec with Matchers {
  val file = new File("/dev/null")
  val oai_url = new java.net.URL("http://aggregator.padigital.org/oai")
  val oai_verb = "ListRecords"
  val prefix = "oai_dc"
  val harvester = new OaiHarvester(endpoint = oai_url,
                                   metadataPrefix = prefix,
                                   outDir = file)

  val xml_valid = la.dp.ingestion3.data.TestOaiData.pa_oai
  val xml_error = la.dp.ingestion3.data.TestOaiData.pa_error

  "buildQueryUrl (when not given a resumptionToken) " should " return a URL" in {
    val q_url = harvester.buildQueryUrl(verb="ListRecords")
    assert(q_url.isInstanceOf[URL])
  }

  "buildQueryUrl (when given a resumptionToken) " should " return a URL" in {
    val q_url = harvester.buildQueryUrl(resumptionToken = "pickup1", verb="ListRecords")
    assert(q_url.isInstanceOf[URL])
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

  "getErrorCode " should " return an empty string if there is no error code" in {
      assert(harvester.getErrorCode(xml_valid).isEmpty)
  }

  it should " return a non-Empty string if there is an error code " in {
    val errorCode = harvester.getErrorCode(xml_error)
    assert(errorCode.nonEmpty)
  }

  "getHarvestedRecords " should " return a map of size 10 from data.pa_oai" in {
    assert(harvester.getHarvestedRecords(xml_valid).size == 10)
  }

  "getResumptionToken() " should " return an empty String if there is an error in the response " in {
    assert(harvester.getResumptionToken(xml_error).isEmpty)
  }

  it should " return a non-empty String value if the response is valid and incomplete" in {
    assert(harvester.getResumptionToken(xml_valid).nonEmpty)
  }
  it should " equal '90d421891feba6922f57a59868d7bcd1'" in {
    assert(harvester.getResumptionToken(xml_valid) == "90d421891feba6922f57a59868d7bcd1")
  }

  "getXmlResponse (when given an invalid URL) " should " throw a HarvesterException exception" in {
    val bad_url = new java.net.URL("http://aggregator.dev.fake/oai")
    val bad_harvester = new OaiHarvester( endpoint = bad_url,
                                          metadataPrefix = prefix,
                                          outDir = file)

    val q_url = bad_harvester.buildQueryUrl(resumptionToken = "", verb = oai_verb)
    assertThrows[HarvesterException] { bad_harvester.getXmlResponse(q_url) }
  }
}
