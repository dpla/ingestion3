package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.messages.{IngestMessage, MessageCollector}
import dpla.ingestion3.model.{nameOnlyPlace, _}
import dpla.ingestion3.utils.FlatFileIO
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods.parse
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

import scala.language.postfixOps

class NYPLMappingTest extends AnyFlatSpec with BeforeAndAfter {

  implicit val msgCollector: MessageCollector[IngestMessage] = new MessageCollector[IngestMessage]
  val shortName: Option[String] = Some("nypl")
  val jsonString: String = new FlatFileIO().readFileAsString("/nypl.json")
  val json: Document[JValue] = Document(parse(jsonString))
  val extractor = new NyplMapping(json)

  it should "extract the correct original ID " in {
    val expected = Some("93cd9a10-c552-012f-20e8-58d385a7bc34")
    assert(extractor.originalId(json) === expected)
  }

  it should "mint the correct DPLA URI" in {
    assert(extractor.dplaUri(json) === Some(URI("http://dp.la/api/items/9412682033a0b7a5926b584e7756019c")))
  }

  it should "use provider prefix" in {
    assert(extractor.useProviderName === true)
  }

  it should "use the correct provider prefix" in {
    assert(extractor.getProviderName === shortName)
  }

  it should "extract the correct place" in {
    val expected = Seq(
      DplaPlace(
        name=Some("Ohio"),
        exactMatch = Seq(URI("http://uri.org/1"))
      ), nameOnlyPlace("Cincinnati"))
    assert(extractor.place(json) === expected)
  }

  it should "extract the correct title " in {
    val expected = Seq("Jedediah Buxton  [National Calculator, 1705-1780]")
    assert(extractor.title(json) === expected)
  }

  it should "extract the correct alt titles" in {
    val expected = Seq("Alternate Title")
    assert(extractor.alternateTitle(json) === expected)
  }

  it should "extract the correct identifiers" in {
    val expected = Seq("URN id")
    assert(extractor.identifier(json) === expected)
  }

  it should "extract the correct description" in {
    val expected = Seq("A valid note", "hello world.")
    assert(extractor.description(json) === expected)
  }

  it should "extract the correct isShownAt value" in {
    val expected = Seq(stringOnlyWebResource("https://digitalcollections.nypl.org/items/4d0e0bc0-c540-012f-1857-58d385a7bc34"))
    assert(extractor.isShownAt(json) === expected)
  }

  it should "extract the correct subjects" in {
//    val expected = Seq("temporal subject", "Public figures", "Subject title", "Subject name", "Ohio", "Cincinnati").map(nameOnlyConcept)

    val expected = Seq(
      nameOnlyConcept("temporal subject"),
      nameOnlyConcept("Public figures"),
      nameOnlyConcept("Subject title"),
      nameOnlyConcept("Subject name"),
      SkosConcept(
        providedLabel = Some("Ohio"),
        exactMatch = Seq(URI("http://uri.org/1"))
      ),
      nameOnlyConcept("Cincinnati")
    )

    assert(extractor.subject(json) forall(expected contains))
  }

  it should "extract the correct temporal values" in {
    val expected = Seq("temporal subject").map(stringOnlyTimeSpan)
    assert(extractor.temporal(json) === expected)
  }

  it should "extract the correct type" in {
    val expected = Seq("still image")
    assert(extractor.`type`(json) === expected)
  }

  it should "extract the correct creator" in {
    val expected = Seq("Kay, John (1742-1826)").map(nameOnlyAgent)
    assert(extractor.creator(json) === expected)
  }

  it should "extract the correct contributor" in {
    val expected = Seq("Contributor").map(nameOnlyAgent)
    assert(extractor.contributor(json) === expected)
  }

  it should "extract the correct dataProvider" in {
    val expected = Seq("The Miriam and Ira D. Wallach Division of Art, Prints and Photographs: Print Collection. The New York Public Library").map(nameOnlyAgent)
    assert(extractor.dataProvider(json) === expected)
  }

  it should "extrac the correct preview" in {
    val expected = Seq("https://images.nypl.org/index.php?t=t&id=G91F088_006F").map(stringOnlyWebResource)
    assert(extractor.preview(json) === expected)
  }

  it should "extract the correct edmRights " in {
    val expected = Seq("http://rightsstatements.org/vocab/NoC-US/1.0/").map(URI)
    assert(extractor.edmRights(json) === expected)
  }

  it should "extract the correct dc rights " in {
    val expected = Seq("The New York Public Library believes that this item is in the public domain under the laws of the United States, but did not make a determination as to its copyright status under the copyright laws of other countries. This item may not be in the public domain under the laws of other countries. Though not required, if you want to credit us as the source, please use the following statement, \"From The New York Public Library,\" and provide a link back to the item on our Digital Collections site. Doing so helps us track how our collection is used and helps justify freely releasing even more content in the future.")
    assert(extractor.rights(json) === expected)
  }

  it should "extract the correct collection name" in {
    val expected = Seq("Robert N. Dennis collection of stereoscopic views").map(nameOnlyCollection)
    assert(extractor.collection(json) === expected)
  }
}
