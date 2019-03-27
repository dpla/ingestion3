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
    val expected = Some("aaa_agpapers_1016")
    assert(extractor.originalId(json) === expected)
  }

  // `type`
  it should "create the correct original type" in {
    val expected = Seq("Text")
    assert(extractor.`type`(json) === expected)
  }

  // collection

  // contributor
  it should "create the correct contributor" in {
    val expected = Seq("Finch, N. P. T.").map(nameOnlyAgent)
    assert(extractor.contributor(json) === expected)
  }

  // creator
  it should "create the correct creator" in {
    val expected = Seq("Manufacturers' Review Co.").map(nameOnlyAgent)
    assert(extractor.creator(json) === expected)
  }

  // dataProvider
  it should "create the correct dataProvider" in {
    val expected = Seq("Auburn University. Library").map(nameOnlyAgent)
    assert(extractor.dataProvider(json) === expected)
  }

  // date

  // description
  it should "create the correct description" in {
    val expected = Seq("description one", "description two")
    assert(extractor.description(json) === expected)
  }

  // dplaUri

  // edmRights

  // extent
  it should "create the correct extent" in {
    val expected = Seq("extent one", "extent two")
    assert(extractor.extent(json) === expected)
  }

  // format
  it should "create the correct format" in {
    val expected = Seq() // expect empty b/c of filters
    assert(extractor.format(json) === expected)
  }

  // identifier
  it should "create the correct identifier" in {
    val expected = Seq("ManufactReview_v01_i05_1899_Dec_14.pdf")
    assert(extractor.identifier(json) === expected)
  }

  // isShownAt

  // language

  // place

  // preview

  // publisher
  it should "create the correct publisher" in {
    val expected = Seq("USAIN State and Local Literature Preservation Project, Special Collections and Archives, Auburn University Libraries, Auburn, Alabama").map(nameOnlyAgent)
    assert(extractor.publisher(json) === expected)
  }

  // relation

  // rightsHolder

  // subject

  // title
  it should "create the correct title" in {
    val expected = Seq("1899-12-14: Manufacturers' Review, Birmingham, Alabama, Volume 1, Issue 5")
    assert(extractor.title(json) === expected)
  }
}