package dpla.ingestion3.model

import dpla.ingestion3.data.EnrichedRecordFixture
import org.scalatest.FlatSpec

class WikiMarkupStringTest extends FlatSpec {

  "buildWikiMarkup" should "generate valid wiki markup for rightstatements.org value"  in {
    val markup: String = buildWikiMarkup(EnrichedRecordFixture.wikimediaEntrichedRecord)
    val expectedMarkup = """== {{int:filedesc}} ==
                        | {{ Artwork
                        |   | Other fields 1 = {{ InFi | Creator | J Doe }}
                        |   | title = The Title
                        |   | description = The description
                        |   | date = 2012-05-07
                        |   | permission = {{NoC-US | Q83878447}}
                        |   | source = {{ DPLA
                        |       | Q83878447
                        |       | hub = The Provider
                        |       | url = https://example.org/record/123
                        |       | dpla_id = 4b1bd605bd1d75ee23baadb0e1f24457
                        |       | local_id = us-history-13243; j-doe-archives-2343
                        |   }}
                        |   | Institution = {{ Institution | wikidata = Q83878447 }}
                        | }}""".stripMargin
    assert(expectedMarkup === markup)
  }

  it should "print valid wiki markup for CC licenses" in {
    val whitespace = " "
    val markup: String = buildWikiMarkup(EnrichedRecordFixture.wikimediaEntrichedRecord.copy(
      edmRights = Some(URI("http://creativecommons.org/licenses/by-sa/1.0/"))
    ))
    val expectedMarkup = s"""== {{int:filedesc}} ==
                           | {{ Artwork
                           |   | Other fields 1 = {{ InFi | Creator | J Doe }}
                           |   | title = The Title
                           |   | description = The description
                           |   | date = 2012-05-07
                           |   | permission = {{Cc-by-sa-1.0}}
                           |   | source = {{ DPLA
                           |       | Q83878447
                           |       | hub = The Provider
                           |       | url = https://example.org/record/123
                           |       | dpla_id = 4b1bd605bd1d75ee23baadb0e1f24457
                           |       | local_id = us-history-13243; j-doe-archives-2343
                           |   }}
                           |   | Institution = {{ Institution | wikidata = Q83878447 }}
                           | }}""".stripMargin
    assert(expectedMarkup === markup)
  }

  "escapeWikiChars" should "escape '{{'" in {
    val value = "Title with {{ and }}"
    val expectedValue = "Title with <nowiki>{{</nowiki> and <nowiki>}}</nowiki>"

    assert(escapeWikiChars(value) === expectedValue)
  }

  "licenseToMarkupCode" should " create a valid wiki code for /by-sa/1.0/ " in {
    val uri = "http://creativecommons.org/licenses/by-sa/1.0/"
    assert(licenseToMarkupCode(uri) === "Cc-by-sa-1.0")
  }
  it should " create a valid wiki code for by-sa/2.5/scotland/" in {
    val uri = "http://creativecommons.org/licenses/by-sa/2.5/scotland/"
    assert(licenseToMarkupCode(uri) === "Cc-by-sa-2.5-scotland")
  }
  it should " create a valid wiki code for by/3.0/igo/" in {
    val uri = "http://creativecommons.org/licenses/by/3.0/igo/"
    assert(licenseToMarkupCode(uri) === "Cc-by-3.0-igo")
  }
}
