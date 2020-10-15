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
  it should "create the correct collection " in {
    val expected = Seq("Auburn University - Agriculture and Rural Life Newspapers Collection").map(nameOnlyCollection)
    assert(extractor.collection(json) === expected)
  }

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
  it should "create the correct date" in {
    val expected = Seq("1899-12-14").map(stringOnlyTimeSpan)
    assert(extractor.date(json) === expected)
  }

  // description
  it should "create the correct description" in {
    val expected = Seq("description one", "description two")
    assert(extractor.description(json) === expected)
  }

  // dplaUri
  it should "create the correct dpla URI" in {
    val expected = Some(URI("http://dp.la/api/items/e941d6996f5b49d6d15578d542b886e7"))
    assert(extractor.dplaUri(json) === expected)
  }

  // edmRights
  it should "create the correct edmRights" in {
    val expected = List(URI("http://rightsstatements.org/vocab/CNE/1.0/"))
    assert(extractor.edmRights(json) === expected)
  }

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
  it should "create the correct isShownAt" in {
    val expected = Seq("http://content.lib.auburn.edu/cdm/ref/collection/agpapers/id/1016").map(stringOnlyWebResource)
    assert(extractor.isShownAt(json) === expected)
  }

  // language
  it should "create the correct language" in {
    val expected = Seq("eng").map(nameOnlyConcept)
    assert(extractor.language(json) === expected)
  }

  // place
  it should "create the correct place" in {
    val expected = Seq("United States, Alabama, Jefferson County, Birmingham, 33.5206608, -86.80249").map(nameOnlyPlace)
    assert(extractor.place(json) === expected)
  }

  // preview
  it should "create the correct previews" in {
    val expected = Seq("edm_is_shown_by_display").map(stringOnlyWebResource)
    assert(extractor.preview(json) === expected)
  }

  it should "create the correct preview when `edm_is_shown_by_display` is missing" in {
    val jsonString: String = new FlatFileIO().readFileAsString("/dlg_missing_preview.json")
    val json: Document[JValue] = Document(parse(jsonString))

    val expected = Seq("https://dlg.galileo.usg.edu/do-th:aaa_agpapers_1016").map(stringOnlyWebResource)
    assert(extractor.preview(json) === expected)
  }

  // publisher
  it should "create the correct publisher" in {
    val expected = Seq("USAIN State and Local Literature Preservation Project, Special Collections and Archives, Auburn University Libraries, Auburn, Alabama").map(nameOnlyAgent)
    assert(extractor.publisher(json) === expected)
  }

  // relation
  it should "create the correct relation" in {
    val expected = Seq("Deeply Rooted", "USAIN State and Local Literature Preservation Project").map(eitherStringOrUri)
    assert(extractor.relation(json) === expected)
  }

  // rightsHolder
  it should "create the correct rightsHolder" in {
    val expected = Seq("This image is the property of the Auburn University Libraries and is intended for non-commercial use. Users of the image are asked to acknowledge the Auburn University Libraries.").map(nameOnlyAgent)
    assert(extractor.rightsHolder(json) === expected)
  }

  // subject
  it should "create the correct subject" in {
    val expected = Seq(
      "Technology--Southern States--Periodicals",
      "Industries--Southern States",
      "Southern States--Economic conditions--Periodicals",
      "American newspapers--Southern States;",
      "Business & Industry",
      "Science & Technology").map(nameOnlyConcept)
    assert(extractor.subject(json) === expected)
  }

  // title
  it should "create the correct title" in {
    val expected = Seq("1899-12-14: Manufacturers' Review, Birmingham, Alabama, Volume 1, Issue 5")
    assert(extractor.title(json) === expected)
  }
}