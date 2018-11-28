
package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.model.{DcmiTypeCollection, _}
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

  it should "extract the correct identifier" in {
    val expected = "missouri--urn:data.mohistory.org:msu_all:oai:digitalcollections.missouristate.edu:Hennicke/94"
    assert(extractor.getProviderId(json) === expected)
  }

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
  it should "extract the correct object" in {
    val expected = List(stringOnlyWebResource("http://digitalcollections.missouristate.edu/utils/getthumbnail/collection/Hennicke/id/94"))
    assert(extractor.`object`(json) === expected)
  }
}
