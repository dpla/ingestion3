package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.messages.{IngestMessage, MessageCollector}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods._
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class MwdlMappingTest extends AnyFlatSpec with BeforeAndAfter {

  implicit val msgCollector: MessageCollector[IngestMessage] = new MessageCollector[IngestMessage]
  val shortName = "mwdl"
  val jsonString: String = new FlatFileIO().readFileAsString("/mwdl.json")
  val json: Document[JValue] = Document(parse(jsonString))
  val extractor = new MwdlMapping

  it should "use the provider shortname in minting IDs" in
    assert(extractor.useProviderName)

  it should "extract the correct original identifier" in
    assert(extractor.originalId(json) === Some("digcoll_slc_27works_598"))

  it should "extract the correct collection titles" in {
    val expected = Seq("Salt Lake Community College Scholarly and Creative Works")
      .map(nameOnlyCollection)
    assert(extractor.collection(json) === expected)
  }

  it should "extract the correct contributors" in {
    val expected = Seq("Salt Lake Community College (Creator)").map(nameOnlyAgent)
    assert(extractor.contributor(json) === expected)
  }

  it should "extract the correct dates" in {
    val expected = Seq("2012").map(stringOnlyTimeSpan)
    assert(extractor.date(json) === expected)
  }

  it should "extract the correct description" in
    assert(extractor.description(json) === Seq("Fall 2012 issue of Folio."))

  it should "extract the correct format" in
    assert(extractor.format(json) === Seq("photograph"))

  it should "extract the correct place values" in {
    val expected = Seq("Place").map(nameOnlyPlace)
    assert(extractor.place(json) === expected)
  }

  it should "extract the correct rights value" in
    assert(extractor.rights(json) === Seq("https://creativecommons.org/licenses/by-nc/4.0/"))

  it should "extract the correct subjects" in {
    val expected = Seq("Art", "Literature", "Poetry", "writing").map(nameOnlyConcept)
    assert(extractor.subject(json) === expected)
  }

  it should "extract the correct titles" in
    assert(extractor.title(json) === Seq("Folio: To Do Something With the Sky"))

  it should "extract the correct types" in
    assert(extractor.`type`(json) === Seq("text"))

  it should "extract the correct dataProvider" in {
    val expected = Seq(nameOnlyAgent("Salt Lake Community College Libraries"))
    assert(extractor.dataProvider(json) === expected)
  }

  it should "extract the correct edmRights" in {
    val expected = Seq(URI("http://rightsstatements.org/vocab/CNE/1.0/"))
    assert(extractor.edmRights(json) === expected)
  }

  it should "extract the correct isShownAt from delivery" in {
    val expected = Seq(
      stringOnlyWebResource("https://utah-primo.hosted.exlibrisgroup.com/permalink/01UTAH_INST/MWDL/digcoll_slc_27works_598")
    )
    assert(extractor.isShownAt(json) === expected)
  }

  it should "fall back to constructed URL when availabilityLinksUrl is absent" in {
    val minimalJson = parse("""
      {
        "pnx": {
          "control": { "recordid": ["digcoll_slc_27works_598"] },
          "display": {}
        },
        "delivery": {}
      }
    """)
    val expected = Seq(
      stringOnlyWebResource(
        "https://utah-primo.hosted.exlibrisgroup.com/permalink/01UTAH_INST/MWDL/digcoll_slc_27works_598"
      )
    )
    assert(extractor.isShownAt(Document(minimalJson)) === expected)
  }

  it should "extract the correct preview thumbnails" in {
    val expected = Seq(
      "https://libarchive.slcc.edu/islandora/object/works_598/datastream/TN/",
      "https://libarchive.slcc.edu/islandora/object/works_598/datastream/TN/"
    ).map(stringOnlyWebResource)
    assert(extractor.preview(json) === expected)
  }

  it should "create the correct DPLA URI" in {
    val expected = Some(URI("http://dp.la/api/items/5c31abd09b535552592bf97cbed6557a"))
    assert(extractor.dplaUri(json) === expected)
  }

  it should "apply nwdh tags" in {
    val tagJson = parse("""
      {
        "pnx": {
          "control": { "recordid": ["test-id"] },
          "display": { "lds03": ["Bushnell University"] }
        },
        "delivery": {}
      }
    """)
    val expected = Seq(URI("nwdh"))
    assert(extractor.tags(Document(tagJson)) === expected)
  }
}
