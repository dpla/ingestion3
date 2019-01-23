package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import org.json4s._

import org.scalatest.{BeforeAndAfter, FlatSpec}

class CdlMappingTest extends FlatSpec with BeforeAndAfter {
  val extractor = new CdlMapping

  it should "extract the correct original id" in {
    val json = org.json4s.jackson.JsonMethods.parse("""{ "id": "foo" }""")
    val expected = "foo"
    assert(extractor.originalId(Document(json)) == expected)
  }
}