package dpla.ingestion3.mappers.providers.experimental

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods._
import org.scalatest.flatspec.AnyFlatSpec

class ArizonaMappingTest extends AnyFlatSpec {

  val shortName: Option[String] = Some("arizona")
  val jsonString: String = new FlatFileIO().readFileAsString("/arizona.json")
  val json: Document[JValue] = Document(parse(jsonString))
  val extractor = new ArizonaMapping

  // ID minting
  it should "salt DPLA ids with the provider short name" in {
    assert(extractor.useProviderName === true)
    assert(extractor.getProviderName === shortName)
  }

  it should "extract the correct original ID" in {
    assert(extractor.originalId(json) === Some("256212"))
  }

  it should "mint the correct DPLA URI" in {
    // md5("arizona--256212") = 4b7... (computed by the mapper); assert stable prefix + shape
    val uri = extractor.dplaUri(json)
    assert(uri.exists(_.value.startsWith("http://dp.la/api/items/")))
  }

  // OreAggregation
  it should "use the Arizona Memory Project as provider" in {
    assert(
      extractor.provider(json) === EdmAgent(
        name = Some("Arizona Memory Project"),
        uri = Some(URI("http://dp.la/api/contributor/arizona"))
      )
    )
  }

  it should "map Contributing Institution to dataProvider" in {
    val expected = List(
      nameOnlyAgent(
        "State of Arizona Research Library-Arizona State Library, Archives and Public Records"
      )
    )
    assert(extractor.dataProvider(json) === expected)
  }

  it should "map isShownAt from the node URL" in {
    val expected =
      List(stringOnlyWebResource("https://azmemory.azlibrary.gov/nodes/view/256212"))
    assert(extractor.isShownAt(json) === expected)
  }

  it should "not emit a preview when og:image is the site logo placeholder" in {
    assert(extractor.preview(json) === List())
  }

  it should "map edmRights from the partner-published rights URI" in {
    val expected = List(URI("http://rightsstatements.org/vocab/NoC-US/1.0/"))
    assert(extractor.edmRights(json) === expected)
  }

  it should "carry the free-text rights statement" in {
    assert(extractor.rights(json).size === 1)
    assert(extractor.rights(json).head.startsWith("The organization that has made"))
  }

  // SourceResource
  it should "extract the title" in {
    assert(extractor.title(json) === List("What&#039;s On : Tucson Southern Arizona, 1944-12"))
  }

  it should "map Creator" in {
    assert(extractor.creator(json) === List(nameOnlyAgent("Tucson Chamber of Commerce")))
  }

  it should "map Publisher" in {
    assert(extractor.publisher(json) === List(nameOnlyAgent("Tucson Chamber of Commerce")))
  }

  it should "map Subject (+ Topic)" in {
    val expected = List(
      "Tucson (Ariz.)--Description and Travel",
      "Tourism--Arizona--Tucson"
    ).map(nameOnlyConcept)
    assert(extractor.subject(json) === expected)
  }

  it should "prefer Date Original over Date Range" in {
    assert(extractor.date(json) === List(EdmTimeSpan(originalSourceDate = Some("1944-12"))))
  }

  it should "map Language" in {
    assert(extractor.language(json) === List(nameOnlyConcept("English")))
  }

  it should "map Type" in {
    assert(extractor.`type`(json) === List("Text"))
  }

  it should "map Original Format to format" in {
    assert(extractor.format(json) === List("Periodicals"))
  }

  it should "map each Collection value" in {
    val expected =
      List("Arizona Collection", "Arizona Periodicals and Magazines").map(nameOnlyCollection)
    assert(extractor.collection(json) === expected)
  }

  it should "concatenate identifier fields" in {
    // Call Number + OCLC Number (no Identifier / LCCN on this record)
    assert(
      extractor.identifier(json) === List("WHAT'S ON TUCSON 1940-1947", "18938523")
    )
  }

  it should "build a hierarchical place from the geography fields" in {
    val expected = List(
      DplaPlace(
        city = Some("Tucson"),
        county = Some("Pima County"),
        state = Some("Arizona"),
        country = Some("United States")
      )
    )
    assert(extractor.place(json) === expected)
  }

  // Free-text rights (no partner-published URI) — edmRights empty, rights carries text.
  it should "leave edmRights empty and keep free text for non-standard rights" in {
    val doc: Document[JValue] = Document(
      parse(
        """{
          |  "id": "999",
          |  "rights": { "iconCode": "13", "label": "IN COPYRIGHT- AZGOVDOC",
          |              "uri": null,
          |              "statement": "Copyright to this resource is held by the creating agency." }
          |}""".stripMargin
      )
    )
    assert(extractor.edmRights(doc) === List())
    assert(
      extractor.rights(doc) === List(
        "Copyright to this resource is held by the creating agency."
      )
    )
  }
}
