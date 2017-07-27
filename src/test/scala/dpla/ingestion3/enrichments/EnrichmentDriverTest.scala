package dpla.ingestion3.enrichments

import java.net.URI


import dpla.ingestion3.model._
import dpla.ingestion3.data.MappedRecordsFixture
import dpla.ingestion3.model.{DplaSourceResource, EdmTimeSpan, SkosConcept}
import org.scalatest.{BeforeAndAfter, FlatSpec}

class EnrichmentDriverTest extends FlatSpec with BeforeAndAfter {

  val driver = new EnrichmentDriver

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
      uri = new URI(""), //uri of the record on our site
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
        providedLabel = Some("eng"),
        concept = Some("English"),
        scheme = Some(new URI("http://lexvo.org/id/iso639-3/")))
      )
    ))

    val enrichedRecord = driver.enrich(MappedRecordsFixture.mappedRecord)

    assert(enrichedRecord === expectedValue)
  }
}


