package dpla.ingestion3.harvesters.oai

import org.scalatest._
import org.scalamock.scalatest.MockFactory

/**
  * Tests for OaiResponseProcessor
  */
class OaiResponseProcessorTest extends FlatSpec with MockFactory {

  val validRecordOaiXml = dpla.ingestion3.data.TestOaiData.paOaiListRecordsRsp
  val validSetOaiXml = dpla.ingestion3.data.TestOaiData.inOaiListSetsRsp
  val errorOaiXml = dpla.ingestion3.data.TestOaiData.paOaiErrorRsp

  "getRecords" should "return the correct number of records" in {
    val records = OaiResponseProcessor.getRecords(validRecordOaiXml)
    assert(records.size === 10)
  }
  it should "add OaiSet to OaiRecord if OaiSet is given" in {
    val mockOaiSet = mock[OaiSet]
    val records = OaiResponseProcessor.getRecords(validRecordOaiXml, Some(mockOaiSet))
    assert(records(0).set.nonEmpty)
  }
  it should "return empty sequence if XML page contains no records" in {
    val records = OaiResponseProcessor.getRecords(validSetOaiXml)
    assert(records.size === 0)
  }
  it should "parse record id" in {
    val expectedId = "oai:libcollab.temple.edu:fedora-system:ContentModel-3.0"
    val records = OaiResponseProcessor.getRecords(validRecordOaiXml)
    // Use contains instead of === to handle whitespace in test data.
    assert(records(0).id.contains(expectedId))
  }

  "getSets" should "return the correct number of sets" in {
    val sets = OaiResponseProcessor.getSets(validSetOaiXml)
    assert(sets.size === 5)
  }
  it should "return empty sequence if XML page contains no sets" in {
    val sets = OaiResponseProcessor.getSets(validRecordOaiXml)
    assert(sets.size === 0)
  }
  it should "parse set id" in {
    val expectedId = "PALNI_winona"
    val sets = OaiResponseProcessor.getSets(validSetOaiXml)
    assert(sets(0).id === expectedId)
  }

  "getResumptionToken" should "return the resumption token" in {
    val expectedToken = "90d421891feba6922f57a59868d7bcd1"
    val token = OaiResponseProcessor.getResumptionToken(validRecordOaiXml.toString)
    assert(token === Some(expectedToken))
  }
  it should "return None in absence of resumption token" in {
    val token = OaiResponseProcessor.getResumptionToken(validSetOaiXml.toString)
    assert(token === None)
  }

  "getOaiErrorCode" should "return Unit if there is no error code" in {
    val error = OaiResponseProcessor.getOaiErrorCode(validRecordOaiXml)
    assert(error.isInstanceOf[Unit])
  }
  it should "throw an Exception if there is an error code" in {
    assertThrows[Exception](OaiResponseProcessor.getOaiErrorCode(errorOaiXml))
  }
}
