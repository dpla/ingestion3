
package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods._
import org.scalatest.{BeforeAndAfter, FlatSpec}

class FlMappingTest extends FlatSpec with BeforeAndAfter {

  val shortName = "florida"
  val jsonString: String = new FlatFileIO().readFileAsString("/fl.json")
  val json: Document[JValue] = Document(parse(jsonString))
  val extractor = new FlMapping


  it should "use the provider short name when minting DPLA ids" in {
    assert(extractor.useProviderName === true)
  }

  it should "return the correct provider name" in {
    assert(extractor.getProviderName === "florida")
  }

  it should "extract the correct original ID" in {
    val expected = Some("http://purl.flvc.org/fsu/fd/FSUHPUA_2014028_193")
    assert(extractor.originalId(json) === expected)
  }

  // dataProvider
  it should "extract the correct dataProvider" in {
    val expected = List(nameOnlyAgent("Florida State University Libraries"))
    assert(extractor.dataProvider(json) === expected)
  }

  // dplaUri
  it should "create the correct DPLA URI" in {
    val expected = Some(URI("http://dp.la/api/items/62205cee7eb6d1293fc51496c3924e9c"))
    assert(extractor.dplaUri(json) === expected)
  }

  // isShownAt
  it should "extract the correct isShownAt" in {
    val expected = List(stringOnlyWebResource("http://purl.flvc.org/fsu/fd/FSUHPUA_2014028_193"))
    assert(extractor.isShownAt(json) === expected)
  }

  // object
  it should "extract the correct preview" in {
    val expected = List(stringOnlyWebResource("http://fsu.digital.flvc.org/islandora/object/fsu:115737/datastream/TN/view"))
    assert(extractor.preview(json) === expected)
  }

  // creator
  it should "extract the correct creator" in {
    val expected = List("creator 1").map(nameOnlyAgent)
    assert(extractor.creator(json) === expected)
  }

  // description
  it should "extract the correct description" in {
    val expected = List("Supporting Act: HOTTUB")
    assert(extractor.description(json) === expected)
  }

  // format
  it should "extract the correct format" in {
    val expected = List("format", "Posters")
    assert(extractor.format(json) === expected)
  }

  // genre
  it should "extract the correct genre " in {
    val expected = List("Posters").map(nameOnlyConcept)
    assert(extractor.genre(json) === expected)
  }

  // identifier
  it should "extract the correct identifier" in {
    val expected = List("http://purl.flvc.org/fsu/fd/FSUHPUA_2014028_193")
    assert(extractor.identifier(json) === expected)
  }

  // language
  it should "extract the correct language" in {
    val expected = List("English").map(nameOnlyConcept)
    assert(extractor.language(json) === expected)
  }

  // rights
  it should "extract the correct rights" in {
    val expected = List("Use of this item is provided for non-commercial, personal, educational, and research use only. For information about the copyright and reproduction rights for this item, please contact Heritage Protocol & University Archives, Florida State University Libraries, Tallahassee, Florida.")
    assert(extractor.rights(json) === expected)
  }

  // subject
  it should "extract the correct subjects" in {
    val expected = List("Florida State University","Club Downunder (Tallahassee, Fla.)")
      .map(nameOnlyConcept)
    assert(extractor.subject(json) === expected)
  }

  // temporal
  it should "extract the correct date with display, begin and end" in {
    val expected = List(
      EdmTimeSpan(originalSourceDate = Some("2009-03-31"), begin = Some("2009-03-31"), end = Some("2009-03-31"))
    )
    assert(extractor.date(json) === expected)
  }

  // title
  it should "extract the correct title" in {
    val expected = List("The Ting Tings")
    assert(extractor.title(json) === expected)
  }
}
