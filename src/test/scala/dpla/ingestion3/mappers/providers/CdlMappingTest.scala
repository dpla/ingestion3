package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.model.{URI, _}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.json4s.jackson.JsonMethods._

class CdlMappingTest extends AnyFlatSpec with BeforeAndAfter {
  val extractor = new CdlMapping

  it should "create the correct DPLA URI" in {
    val json = parse("""{ "id": "foo" }""")
    val expected = Some(URI("http://dp.la/api/items/38bb94ab57334e6bacbefe5b12c173a5"))
    assert(extractor.dplaUri(Document(json)) === expected)
  }

  it should "extract the correct original id" in {
    val json = parse("""{ "id": "foo" }""")
    val expected = Some("foo")
    assert(extractor.originalId(Document(json)) == expected)
  }

  it should "extract the correct intermediate provider" in {
    val json = parse("""{ "repository_name": [ "foo", "University of Southern California Digital Library" ] }""")
    val expected = Some(nameOnlyAgent("University of Southern California Digital Library"))
    assert(extractor.intermediateProvider(Document(json)) == expected)
  }

  it should "extract no intermediate provider" in {
    val json = parse("""{ "repository_name": [ "foo" ] }""")
    val expected = None
    assert(extractor.intermediateProvider(Document(json)) == expected)
  }
}