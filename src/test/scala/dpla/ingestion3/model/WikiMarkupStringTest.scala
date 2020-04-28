package dpla.ingestion3.model

import dpla.ingestion3.data.EnrichedRecordFixture
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.scalatest.FlatSpec

class WikiMarkupStringTest extends FlatSpec {

  "wikiRecord" should "print a valid JSON string" in {
    val s: String = wikiRecord(EnrichedRecordFixture.wikimediaEntrichedRecord)
    val jvalue = parse(s)
    assert(jvalue.isInstanceOf[JValue])
  }

  "getWikiMarkup" should "print valid wiki markup" in {
    val markup: String = getWikiMarkup(EnrichedRecordFixture.wikimediaEntrichedRecord)

    println(markup)

    val validMarkup = """"""
    assert(validMarkup === markup)
  }



}
