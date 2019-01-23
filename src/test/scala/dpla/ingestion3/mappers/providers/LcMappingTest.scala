package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.messages.{IngestMessage, MessageCollector}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods._
import org.scalatest.{BeforeAndAfter, FlatSpec}

class LcMappingTest extends FlatSpec with BeforeAndAfter {
  implicit val msgCollector: MessageCollector[IngestMessage] = new MessageCollector[IngestMessage]
  val shortName = "loc"
  val jsonString: String = new FlatFileIO().readFileAsString("/lc.json")
  val json: Document[JValue] = Document(parse(jsonString))
  val extractor = new LcMapping

  it should "extract the correct rights" in {
    val expected = Seq("For rights relating to this resource, visit https://www.loc.gov/item/73691632/")
    assert(extractor.rights(json) === expected)
  }

  it should "extract the correct dataProvider" in {
    val expected = Seq(nameOnlyAgent("Library of Congress"))
    assert(extractor.dataProvider(json) === expected)
  }

  it should "extract the correct original id" in {
    val expected = "http://www.loc.gov/item/73691632/"
    assert(extractor.originalId(json) == expected)
  }

  it should "extract the correct URL for isShownAt" in {
    val expected = Seq(stringOnlyWebResource("https://www.loc.gov/item/73691632/"))
    assert(extractor.isShownAt(json) === expected)
  }

  it should "extract the correct url for preview" in {
    val expected = Seq(uriOnlyWebResource(URI("http:images.com")))
    assert(extractor.preview(json) === expected)
  }
  // TODO test extraction of other-titles and alternate_title
  it should "extract the correct alternate title" in {
    val json = Document(parse(
      """{"item": {"other-title": ["alt title"], "other-titles": ["alt title 2"],"alternate_title": ["alt title 3"]}} """))

    assert(extractor.alternateTitle(json) == Seq("alt title"))
  }

  // TODO test correct extraction from `dates` and if both present s
  it should "extract the correct date" in {
    val expected = Seq("1769").map(stringOnlyTimeSpan)
    assert(extractor.date(json) == expected)
  }

  it should "extract the correct description" in {
    val expected = Seq("Scale ca. 1:740,000. Title from verso. Manuscript, pen-and-ink. On verso: No 33. LC Maps of North America, 1750-1789, 1244 Available also through the Library of Congress Web site as a raster image. Vault AACR2", "[1769?]")

    assert(extractor.description(json) == expected)
  }

  it should "extract the correct extent" in {
    val expected = Seq("map, on sheet 39 x 29 cm.")
    assert(extractor.extent(json) == expected)
  }

  // TODO test extraction from [item \ format \ type]
  it should "extract the correct format" in {
    val expected = Seq("map")
    assert(extractor.format(json) == expected)
  }

  it should "extract the correct identifiers" in {
    val expected = Seq("http://www.loc.gov/item/73691632/")
    assert(extractor.identifier(json) == expected)
  }

  it should "extract the correct language" in {
    val expected = Seq("english").map(nameOnlyConcept)
    assert(extractor.language(json) == expected)
  }

  it should "extract the correct location" in {
    val expected = Seq("New jersey", "New york", "New york (state)","United states")
      .map(nameOnlyPlace)
    assert(extractor.place(json) == expected)
  }

  it should "extract the correct title" in {
    val expected = Seq("Lines run in the Jersies for determining boundaries between that Province & New York.")
    assert(extractor.title(json) == expected)
  }

  // TODO Add test for extracting format keys
  it should "extract the correct type" in {
    val expected = Seq("map", "map")
    assert(extractor.`type`(json) == expected)
  }
 }
