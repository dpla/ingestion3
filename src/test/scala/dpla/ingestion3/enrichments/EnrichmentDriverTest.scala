package dpla.ingestion3.enrichments

import java.net.URI

import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.data.MappedRecordsFixture
import dpla.ingestion3.model.{DplaSourceResource, EdmTimeSpan, SkosConcept, _}
import org.scalatest.{BeforeAndAfter, FlatSpec}


class EnrichmentDriverTest extends FlatSpec with BeforeAndAfter {

  val driver = new EnrichmentDriver(i3Conf())

  val mappedRecord = DplaMapData(
    DplaSourceResource(
      date = Seq(EdmTimeSpan(
        originalSourceDate=Some("4.3.2015"),
        prefLabel = None,
        begin = None,
        end = None
      )),
      language = Seq(SkosConcept(
        providedLabel = Some("eng"),
        note = None,
        scheme = None,
        exactMatch= Seq(),
        closeMatch = Seq())
      )
    ),
    EdmWebResource(
      uri = new URI("")
    ),
    OreAggregation(
      dataProvider = EdmAgent(),
      uri = new URI("https://example.org/item/123"), //uri of the record on our site
      originalRecord = "", //map v4 specifies this as a ref, but that's LDP maybe?
      provider = EdmAgent()
    )
  )

  "EnrichmentDriver" should " enrich both language and date" in {
    val expectedValue = MappedRecordsFixture.mappedRecord.copy(new DplaSourceResource(
      date = Seq(new EdmTimeSpan(
          prefLabel = Some("2012-05-07"),
          originalSourceDate = Some("5.7.2012")
        )),
      language = Seq(SkosConcept(

        /*
         * FIXME: I think that providedLabel and concept are reversed.
         * Concept is supposed to be the "The preferred form of the name of the
         * concept," per
         * http://dp.la/info/wp-content/uploads/2015/03/MAPv4.pdf
         * Provided label is what the provider gave us. The language code is
         * not "English", it's "eng", according to
         * http://www.lexvo.org/page/term/eng/English
         */
        providedLabel = Some("eng"),
        concept = Some("English"),
        scheme = Some(new URI("http://lexvo.org/id/iso639-3/")))

      )
    ))

    val enrichedRecord = driver.enrich(MappedRecordsFixture.mappedRecord)

    assert(enrichedRecord === expectedValue)
  }
}


