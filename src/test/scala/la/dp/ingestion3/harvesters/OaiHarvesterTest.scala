package la.dp.ingestion3.harvesters

import java.io.File
import java.net.URL

import la.dp.ingestion3.utils.FlatFileIO
import org.scalatest._

import scala.xml.NodeSeq


/**
  * Tests for OaiHarvester
  *
  * TODO: Figure out how to simulate http requests/responses.
  *
  */
class OaiHarvesterTest extends FlatSpec with Matchers with BeforeAndAfter {

  val validOaiXml = la.dp.ingestion3.data.TestOaiData.paOaiListRecordsRsp
  val invalidOaiXml = la.dp.ingestion3.data.TestOaiData.paOaiErrorRsp

  val outDir = new File("/dev/null")
  val oaiUrl = new java.net.URL("http://aggregator.padigital.org/oai")
  val oaiVerb = "ListRecords"
  val prefix = "oai_dc"
  val fileIO = new FlatFileIO

  val harvester = new OaiHarvester(endpoint = oaiUrl,
    metadataPrefix = prefix,
    outDir = outDir,
    fileIO = fileIO)

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

  "getHarvestedRecords " should " return a map of size 10 from data.paOaiListRecordsRsp" in {
    assert(harvester.getHarvestedRecords(validOaiXml).size === 10)
  }

  "getResumptionToken() " should " return an empty String if there is an error in the response " in {
    assert(harvester.getResumptionToken(invalidOaiXml).isEmpty)
  }

  it should " return a non-empty String value if the response is valid and incomplete" in {
    assert(harvester.getResumptionToken(validOaiXml).nonEmpty)
  }
  it should " equal '90d421891feba6922f57a59868d7bcd1'" in {
    assert(harvester.getResumptionToken(validOaiXml) === "90d421891feba6922f57a59868d7bcd1")
  }

  // TODO Figure out mocks
  "getXmlResponse (when given an valid URL ) " should " return a NodeSeq" in {
    val xml: NodeSeq = validOaiXml
    assert(xml.isInstanceOf[NodeSeq])
  }

  /**
    * TODO Figure out mocks...
    */
  it should " throw a SaxParserException when given bad XML" in {
    // harvester.getXmlResponse()
  }
}
