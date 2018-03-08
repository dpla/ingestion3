package dpla.ingestion3.enrichments

import java.net.URI

import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.data.MappedRecordsFixture
import dpla.ingestion3.model.{DplaSourceResource, EdmTimeSpan, SkosConcept, _}
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.util.Success


class EnrichmentDriverTest extends FlatSpec with BeforeAndAfter {

  val driver = new EnrichmentDriver(i3Conf())

  "EnrichmentDriver" should " enrich both language and date" in {
    val expectedValue = MappedRecordsFixture.mappedRecord.copy(sourceResource = DplaSourceResource(
      date = Seq(new EdmTimeSpan(
        prefLabel = Some("2012-05-07"),
        originalSourceDate = Some("5.7.2012")
      )),
      language = Seq(
        SkosConcept(
          providedLabel = Some("Eng"),
          concept = Some("English")
        )
      ),
      `type` = Seq("sound")
      )
    )

    val enrichedRecord = driver.enrich(MappedRecordsFixture.mappedRecord)

    assert(enrichedRecord === Success(expectedValue))
  }
}


