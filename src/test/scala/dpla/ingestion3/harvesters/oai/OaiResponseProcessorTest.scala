package dpla.ingestion3.harvesters.oai

import org.scalatest._
import org.scalamock.scalatest.MockFactory

/**
  * Tests for OaiResponseProcessor
  */
class OaiResponseProcessorTest extends FlatSpec with Matchers with MockFactory {

  val fakeParams = Map("foo"-> "bar")
  val fakeUrl = "http://example.org"

  val validRecordSource: OaiSource = {
    val text = dpla.ingestion3.data.TestOaiData.paOaiListRecordsRsp
    OaiSource(fakeParams, Some(fakeUrl), Some(text))
  }

  val validSetSource: OaiSource = {
    val text = dpla.ingestion3.data.TestOaiData.inOaiListSetsRsp
    OaiSource(fakeParams, Some(fakeUrl), Some(text))
  }

  val errorSource: OaiSource = {
    val text = dpla.ingestion3.data.TestOaiData.paOaiErrorRsp
    OaiSource(fakeParams, Some(fakeUrl), Some(text))
  }

  val invalidXmlSource: OaiSource = {
    val text = dpla.ingestion3.data.TestOaiData.badXmlStr
    OaiSource(fakeParams, Some(fakeUrl), Some(text))
  }

  "getRecords" should "return a RecordsPage if source contains records" in {
    val response = OaiResponseProcessor.getRecords(validRecordSource, false)
    assert(response.getClass.getName === "dpla.ingestion3.harvesters.oai.RecordsPage")
  }
  it should "return an OaiError if source contains OAI error message" in {
    val response = OaiResponseProcessor.getRecords(errorSource, false)
    assert(response.getClass.getName === "dpla.ingestion3.harvesters.oai.OaiError")
  }
  it should "return an OaiError if source contains invalid XML" in {
    val response = OaiResponseProcessor.getRecords(invalidXmlSource, false)
    assert(response.getClass.getName === "dpla.ingestion3.harvesters.oai.OaiError")
  }
  it should "return the correct number of records" in {
    val response = OaiResponseProcessor.getRecords(validRecordSource, false)
    response match {
      case r: RecordsPage => assert(r.records.size === 10)
      case _ => fail
    }
  }
  it should "return empty sequence if XML page contains no records" in {
    val response = OaiResponseProcessor.getRecords(validSetSource, false)
    response match {
      case r: RecordsPage => assert(r.records.size === 0)
      case _ => fail
    }
  }
  it should "parse record ids" in {
    val expectedId = "oai:libcollab.temple.edu:fedora-system:ContentModel-3.0"
    val response = OaiResponseProcessor.getRecords(validRecordSource, false)
    response match {
      // Use contains instead of === to handle whitespace in test data.
      case r: RecordsPage => assert(r.records(0).id.contains(expectedId))
      case _ => fail
    }
  }
  it should "parse set ids" in {
    val expectedId = "foobar"
    val response = OaiResponseProcessor.getRecords(validRecordSource, false)
    response match {
      // Use contains instead of === to handle whitespace in test data.
      case r: RecordsPage => assert(r.records(0).setIds(0).contains(expectedId))
      case _ => fail
    }
  }

  "getAllSets" should "return a SetsResponse if source contains sets" in {
    val response = OaiResponseProcessor.getAllSets(validSetSource)
    assert(response.getClass.getName === "dpla.ingestion3.harvesters.oai.SetsPage")
  }
  it should "return an OaiError if source contains OAI error message" in {
    val response = OaiResponseProcessor.getAllSets(errorSource)
    assert(response.getClass.getName === "dpla.ingestion3.harvesters.oai.OaiError")
  }
  it should "return an OaiError if source contains invalid XML" in {
    val response = OaiResponseProcessor.getAllSets(invalidXmlSource)
    assert(response.getClass.getName === "dpla.ingestion3.harvesters.oai.OaiError")
  }
  it should "return the correct number of sets" in {
    val response = OaiResponseProcessor.getAllSets(validSetSource)
    response match {
      case s: SetsPage => assert(s.sets.size === 5)
      case _ => fail
    }
  }
  it should "return empty sequence if XML page contains no sets" in {
    val response = OaiResponseProcessor.getAllSets(validRecordSource)
    response match {
      case s: SetsPage => assert(s.sets.size === 0)
      case _ => fail
    }
  }
  it should "parse set id" in {
    val expectedId = "PALNI_winona"
    val response = OaiResponseProcessor.getAllSets(validSetSource)
    response match {
      case s: SetsPage => assert(s.sets(0).id === expectedId)
      case _ => fail
    }
  }

  "getResumptionToken" should "return the resumption token" in {
    val expectedToken = "90d421891feba6922f57a59868d7bcd1"
    val text = validRecordSource.text.getOrElse("")
    val token = OaiResponseProcessor.getResumptionToken(text)
    val token = OaiResponseProcessor.getResumptionToken(text)
    assert(token === Some(expectedToken))
  }
  it should "return None in absence of resumption token" in {
    val text = validSetSource.text.getOrElse(fail)
    val token = OaiResponseProcessor.getResumptionToken(text)
    assert(token === None)
  }

  "isDeleted" should "returns false if not marked as deleted" in {
    val xml = <oai><header><record></record></header></oai>
    val status = OaiResponseProcessor.isDeleted(xml)
    assert(status === false)
  }
  it should "returns true if marked as deleted" in {
    val xml = <oai><header status="deleted"><record></record></header></oai>
    val status = OaiResponseProcessor.isDeleted(xml)
    assert(status === true)
  }
}
