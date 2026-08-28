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
    assert(extractor.originalId(json) === Some("alma9942890284402001"))

  it should "extract the correct collection title from addtitle" in {
    val expected = Seq("Utah Ski and Snow Sports Photo Archives").map(nameOnlyCollection)
    assert(extractor.collection(json) === expected)
  }

  it should "extract the correct dates (collapsing expanded year ranges)" in {
    // "2008; 2009" are consecutive years → collapsed to "2008-2009"
    val expected = Seq("2008-2009").map(stringOnlyTimeSpan)
    assert(extractor.date(json) === expected)
  }

  it should "extract the correct description" in
    assert(extractor.description(json) === Seq("Color photograph of the University of Utah Alpine skier, Kyle Kung."))

  it should "extract the correct format" in
    assert(extractor.format(json) === Seq("image/jpeg"))

  it should "extract parsed identifiers (stripping $$C...$$V markers)" in {
    val expected = Seq(
      "https://collections.lib.utah.edu/ark:/87278/s6qpdh27",
      "oai:collections.lib.utah.edu:uum_map_usa/2762300"
    )
    assert(extractor.identifier(json) === expected)
  }

  it should "extract the correct subjects" in {
    val expected = Seq(
      "Utah",
      "Ski team",
      "University of Utah. Ski team--Photographs",
      "Skiers--Photographs"
    ).map(nameOnlyConcept)
    assert(extractor.subject(json) === expected)
  }

  it should "extract the correct titles" in
    assert(extractor.title(json) === Seq("008-2009 University of Utah Alpine skier, Kyle Kung"))

  it should "extract the correct types" in
    assert(extractor.`type`(json) === Seq("still image"))

  it should "extract rights (non-URI) as empty when only URI rights are present" in
    assert(extractor.rights(json) === Seq.empty)

  it should "extract the correct edmRights from display.rights URI values" in {
    val expected = Seq(URI("http://rightsstatements.org/vocab/InC-NC/1.0/"))
    assert(extractor.edmRights(json) === expected)
  }

  it should "extract dataProvider from electronicServices.packageName (stripping prefix)" in {
    val expected = Seq(nameOnlyAgent("University of Utah J. Willard Marriott Digital Library"))
    assert(extractor.dataProvider(json) === expected)
  }

  it should "prefer lds03 over electronicServices for dataProvider when present" in {
    val withLds03 = parse("""
      {
        "pnx": {
          "control": { "recordid": ["alma123"] },
          "display": { "lds03": ["Utah State University Merrill-Cazier Library"] }
        },
        "delivery": {
          "electronicServices": [
            { "packageName": "Display resource from Some Other Library" }
          ]
        }
      }
    """)
    val expected = Seq(nameOnlyAgent("Utah State University Merrill-Cazier Library"))
    assert(extractor.dataProvider(Document(withLds03)) === expected)
  }

  it should "extract the correct isShownAt from delivery.availabilityLinksUrl" in {
    val expected = Seq(
      stringOnlyWebResource("https://collections.lib.utah.edu/ark:/87278/s6qpdh27")
    )
    assert(extractor.isShownAt(json) === expected)
  }

  it should "fall back to lds10 URL when availabilityLinksUrl is absent" in {
    val minimalJson = parse("""
      {
        "pnx": {
          "control": { "recordid": ["alma123"] },
          "display": {
            "lds10": ["https://collections.lib.utah.edu/ark:/87278/s6qpdh27", "oai:foo"]
          }
        },
        "delivery": {}
      }
    """)
    val expected = Seq(
      stringOnlyWebResource("https://collections.lib.utah.edu/ark:/87278/s6qpdh27")
    )
    assert(extractor.isShownAt(Document(minimalJson)) === expected)
  }

  it should "extract the correct preview thumbnail (skipping non-thumbnail links)" in {
    val expected = Seq(
      stringOnlyWebResource("https://collections.lib.utah.edu/thumb?id=2762300")
    )
    assert(extractor.preview(json) === expected)
  }

  it should "create the correct DPLA URI" in {
    val uri = extractor.dplaUri(json)
    assert(uri.isDefined)
    assert(uri.get.value.startsWith("http://dp.la/api/items/"))
  }

  it should "apply nwdh tags from dataProvider" in {
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
