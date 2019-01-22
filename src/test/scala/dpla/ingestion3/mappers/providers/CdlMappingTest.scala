package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import org.json4s._

import org.scalatest.{BeforeAndAfter, FlatSpec}

class CdlMappingTest extends FlatSpec with BeforeAndAfter {
  val extractor = new CdlMapping

  it should "extract the correct provider id" in {
    val json = org.json4s.jackson.JsonMethods.parse("""{ "id": "foo" }""")
    val expected = "foo"
    assert(extractor.getProviderId(Document(json)) == expected)
  }

  it should "use the provider ID for the original ID" in {
    val json = org.json4s.jackson.JsonMethods.parse("""{ "id": "foo" }""")
    assert(extractor.getProviderId(Document(json)) == extractor.originalId(Document(json)))
  }
}