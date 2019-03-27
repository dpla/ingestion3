package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.messages.{IngestMessage, MessageCollector}
import dpla.ingestion3.utils.FlatFileIO
import org.json4s.JsonAST.JValue
import org.json4s._
import org.json4s.jackson.JsonMethods.parse
import org.scalatest.{BeforeAndAfter, FlatSpec}
import dpla.ingestion3.model._

class DlgMappingTest extends FlatSpec with BeforeAndAfter {
  implicit val msgCollector: MessageCollector[IngestMessage] = new MessageCollector[IngestMessage]
  val shortName = "dlg"
  val jsonString: String = new FlatFileIO().readFileAsString("/dlg.json")
  val json: Document[JValue] = Document(parse(jsonString))
  val extractor = new DlgMapping

  // original id
  it should "create the correct original id" in {
    val expected = ???
    assert(extractor.originalId(json) === expected)
  }

  // `type`
  it should "create the correct original type" in {
    val expected = Seq("")
    assert(extractor.`type`(json) === expected)
  }

  // collection

  // contributor
  it should "create the correct contributor" in {
    val expected = Seq("").map(nameOnlyAgent)
    assert(extractor.contributor(json) === expected)
  }

  // creator
  it should "create the correct creator" in {
    val expected = Seq("").map(nameOnlyAgent)
    assert(extractor.creator(json) === expected)
  }

  // dataProvider
  it should "create the correct dataProvider" in {
    val expected = Seq("").map(nameOnlyAgent)
    assert(extractor.dataProvider(json) === expected)
  }

  // date

  // description
  it should "create the correct description" in {
    val expected = Seq("")
    assert(extractor.description(json) === expected)
  }

  // dplaUri

  // edmRights

  // extent
  it should "create the correct extent" in {
    val expected = Seq("")
    assert(extractor.extent(json) === expected)
  }

  // format
  it should "create the correct format" in {
    val expected = Seq("")
    assert(extractor.format(json) === expected)
  }

  // identifier
  it should "create the correct identifer" in {
    val expected = Seq("")
    assert(extractor.identifier(json) === expected)
  }

  // isShownAt

  // language

  // place

  // preview

  // publisher
  it should "create the correct publisher" in {
    val expected = Seq("").map(nameOnlyAgent)
    assert(extractor.publisher(json) === expected)
  }

  // relation

  // rightsHolder

  // subject

  // title
  it should "create the correct title" in {
    val expected = Seq("")
    assert(extractor.title(json) === expected)
  }
}