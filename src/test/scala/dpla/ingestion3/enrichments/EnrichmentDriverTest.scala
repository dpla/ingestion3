package dpla.ingestion3.enrichments

import java.net.URI

import dpla.ingestion3.data.MappedRecordsTest
import dpla.ingestion3.model.{DplaSourceResource, EdmTimeSpan, SkosConcept}
import org.scalatest.{BeforeAndAfter, FlatSpec}

class EnrichmentDriverTest extends FlatSpec with BeforeAndAfter {

  val driver = new EnrichmentDriver

  "EnrichmentDriver" should " enrich both language and date" in {
    val expectedValue = MappedRecordsTest.mappedRecord.copy(new DplaSourceResource(
      date = Seq(new EdmTimeSpan(
          prefLabel = Some("2012-05-07"),
          originalSourceDate = Some("5.7.2012")
        )),
      language = Seq(SkosConcept(
        providedLabel = Option("eng"),
        concept = Option("English"),
        scheme = Option(new URI("http://lexvo.org/id/iso639-3/")))
      )
    ))

    val enrichedRecord = driver.enrich(MappedRecordsTest.mappedRecord)

    assert(enrichedRecord === expectedValue)
  }
}


