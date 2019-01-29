package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.model.URI
import org.json4s._
import org.scalatest.{BeforeAndAfter, FlatSpec}

class CdlMappingTest extends FlatSpec with BeforeAndAfter {
  val extractor = new CdlMapping

  it should "create the correct DPLA URI" in {
    val json = org.json4s.jackson.JsonMethods.parse("""{ "id": "foo" }""")
    val expected = Some(URI("http://dp.la/api/items/38bb94ab57334e6bacbefe5b12c173a5"))
    assert(extractor.dplaUri(Document(json)) === expected)
  }

  it should "extract the correct original id" in {
    val json = org.json4s.jackson.JsonMethods.parse("""{ "id": "foo" }""")
    val expected = Some("foo")
    assert(extractor.originalId(Document(json)) == expected)
  }
}