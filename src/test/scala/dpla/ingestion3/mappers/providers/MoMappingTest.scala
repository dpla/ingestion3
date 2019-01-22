
package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods._
import org.scalatest.{BeforeAndAfter, FlatSpec}

class MoMappingTest extends FlatSpec with BeforeAndAfter {

  val shortName = "mo"
  val jsonString: String = new FlatFileIO().readFileAsString("/mo.json")
  val json: Document[JValue] = Document(parse(jsonString))
  val extractor = new MoMapping


  it should "use the provider short name when minting DPLA ids" in {
    assert(extractor.useProviderName === true)
  }

  it should "return the correct provider name" in {
    assert(extractor.getProviderName === "mo")
  }

  it should "extract the correct providerId" in {
    val expected = "missouri--urn:data.mohistory.org:msu_all:oai:digitalcollections.missouristate.edu:Hennicke/94"
    assert(extractor.getProviderId(json) === expected)
  }

  it should "use the provider ID for the original ID" in
    assert(extractor.getProviderId(json) == extractor.originalId(json).get)

  // dataProvider
  it should "extract the correct dataProvider" in {
    val expected = List(nameOnlyAgent("Missouri State University"))
    assert(extractor.dataProvider(json) === expected)
  }

  // dplaUri
  it should "create the correct DPLA URI" in {
    val expected = new URI("http://dp.la/api/items/8c630431c601bd29753c93d3d8eea6cf")
    assert(extractor.dplaUri(json) === expected)
  }

  // hasView
  it should "extract the correct hasView" in {
    val expected = List(stringOnlyWebResource("http://digitalcollections.missouristate.edu/cdm/ref/collection/Hennicke/id/94"))
    assert(extractor.hasView(json) === expected)
  }

  // isShownAt
  it should "extract the correct isShownAt" in {
    val expected = List(stringOnlyWebResource("http://digitalcollections.missouristate.edu/cdm/ref/collection/Hennicke/id/94"))
    assert(extractor.isShownAt(json) === expected)
  }

  // object
  it should "extract the correct preview" in {
    val expected = List(stringOnlyWebResource("http://digitalcollections.missouristate.edu/utils/getthumbnail/collection/Hennicke/id/94"))
    assert(extractor.preview(json) === expected)
  }

  // creator
  it should "extract the correct creator" in {
    val expected = List("Leo. Feist","Herman Darewski Music").map(nameOnlyAgent)
    assert(extractor.creator(json) === expected)
  }

  // description
  it should "extract the correct description" in {
    val expected = List("Six students posing on a bench.")
    assert(extractor.description(json) === expected)
  }

  // format
  it should "extract the correct format" in {
    val expected = List("Book")
    assert(extractor.format(json) === expected)
  }

  // genre
  it should "extract the correct genre from specType field" in {
    val expected = List("Photograph/Pictorial Works").map(nameOnlyConcept)
    assert(extractor.genre(json) === expected)
  }

  // identifier
  it should "extract the correct identifier" in {
    val expected = List("http://digitalcollections.missouristate.edu/cdm/ref/collection/Hennicke/id/94")
    assert(extractor.identifier(json) === expected)
  }

  // language
  it should "extract the correct language" in {
    val expected = List("eng").map(nameOnlyConcept)
    assert(extractor.language(json) === expected)
  }

  // rights
  it should "extract the correct rights" in {
    val expected = List("Use of digital images found on this website is permitted for private or personal use only.")
    assert(extractor.rights(json) === expected)
  }

  // subject
  it should "extract the correct subjects" in {
    val expected = List("Schools--Missouri--Springfield", " Alternative education", " Springfield (Mo.)", "")
      .map(nameOnlyConcept)
    assert(extractor.subject(json) === expected)
  }

  // temporal
  it should "extract the correct date with display, begin and end" in {
    val expected = List(
      EdmTimeSpan(originalSourceDate = Some("1940-05")),
      EdmTimeSpan(originalSourceDate = Some("1991/1995"), begin = Some("1991"), end = Some("1995"))
    )
    assert(extractor.date(json) === expected)
  }

  // title
  it should "extract the correct title" in {
    val expected = List("Photograph of students posing on bench")
    assert(extractor.title(json) === expected)
  }
}
