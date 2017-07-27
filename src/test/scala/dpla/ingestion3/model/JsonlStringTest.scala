package dpla.ingestion3.model


import org.scalatest.FlatSpec
import dpla.ingestion3.data.EnrichedRecordFixture
import org.json4s._
import org.json4s.jackson.JsonMethods._


class JsonlStringTest extends FlatSpec {

  "jsonlRecord" should "print a valid JSON string" in {
    val s: String = jsonlRecord(EnrichedRecordFixture.enrichedRecord)
    val jvalue = parse(s)
    assert(jvalue.isInstanceOf[JValue])
  }

  it should "render a field that's a String" in {
    val s: String = jsonlRecord(EnrichedRecordFixture.enrichedRecord)
    val jvalue = parse(s)
    assert(
      compact(render(jvalue \ "id")) == "\"7738a840caff15224f50cf17eade27e6\""
    )
  }

  it should "render a field that's a sequence" in {
    val s: String = jsonlRecord(EnrichedRecordFixture.enrichedRecord)
    val jvalue = parse(s)
    val title = jvalue \ "sourceResource" \ "title"
    assert(title.isInstanceOf[JArray])
    assert(compact(render(title(0))) == "\"The Title\"")
  }

  it should "render a field that requires a map() on a sequence" in {
    val s: String = jsonlRecord(EnrichedRecordFixture.enrichedRecord)
    val jvalue = parse(s)
    val collection = jvalue \ "sourceResource" \ "collection"
    assert(collection.isInstanceOf[JArray])
    assert(
      compact(render(collection(0))) == "{\"title\":\"The Collection\"}"
    )
  }

  it should "have empty arrays for fields that have no data" in {
    // Those fields that are optional are 0-n, so they will be arrays.
    val s: String = jsonlRecord(EnrichedRecordFixture.minimalEnrichedRecord)
    val jvalue = parse(s)
    assert(
      compact(render(jvalue \ "sourceResource" \ "collection")) == "[]"
    )
  }

}
