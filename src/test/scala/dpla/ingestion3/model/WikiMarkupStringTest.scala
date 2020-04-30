package dpla.ingestion3.model

import dpla.ingestion3.data.EnrichedRecordFixture
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.scalatest.FlatSpec

class WikiMarkupStringTest extends FlatSpec {

  "wikiRecord" should "generate valid JSON" in {
    val s: String = wikiRecord(EnrichedRecordFixture.wikimediaEntrichedRecord)
    val jvalue = parse(s)
    // println(s)
    assert(jvalue.isInstanceOf[JValue])
  }

  "buildWikiMarkup" should "print valid wiki markup" in {
    val markup: String = buildWikiMarkup(EnrichedRecordFixture.wikimediaEntrichedRecord)
    val expectedMarkup = """== {{int:filedesc}} ==
                        | {{ Artwork
                        |   | Other fields 1 = {{ InFi | Creator | J Doe }}
                        |   | title = The Title
                        |   | description = The description
                        |   | date = 2012-05-07
                        |   | permission = {{PD-US}}
                        |   | source = {{
                        |       DPLA | Q83878447 |
                        |       hub = The Provider |
                        |       url = https://example.org/record/123 |
                        |       dpla_id = 4b1bd605bd1d75ee23baadb0e1f24457 |
                        |       local_id = us-history-13243; j-doe-archives-2343
                        |   }}
                        |   | Institution = {{ Institution | wikidata = Q83878447 }}
                        |   Other fields = {{ InFi | Standardized rights statement | {{ rights statement | http://rightsstatements.org/vocab/NoC-US }} }}
                        | }}""".stripMargin
    assert(expectedMarkup === markup)
  }

  "buildWikiAssets" should "build correct URI when given a IIIF manifest" in {
    val record = EnrichedRecordFixture.minimalEnrichedRecord.copy(
      iiifManifest = Some(URI("https://ark.iiif/item/manifest"))
    )

    val expected = Seq("https://ark.iiif/item/manifest")
    assert(buildWikiAssets(record) === expected)
  }

  it should "build correct URI when given a single mediaMaster value" in {
    val expected = Seq("media_master_1")
    val record = EnrichedRecordFixture.minimalEnrichedRecord.copy(
      mediaMaster = expected.map(stringOnlyWebResource)
    )

    assert(buildWikiAssets(record) === expected)
  }

  it should "build correct URI when given multiple mediaMaster values" in {
    val expected = Seq("media_master_1", "media_master_2", "media_master_3")
    val record = EnrichedRecordFixture.minimalEnrichedRecord.copy(
      mediaMaster = expected.map(stringOnlyWebResource)
    )

    assert(buildWikiAssets(record) === expected)
  }

}
