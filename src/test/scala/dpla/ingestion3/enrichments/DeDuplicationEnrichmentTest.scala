package dpla.ingestion3.enrichments

import dpla.ingestion3.data.MappedRecordsFixture
import dpla.ingestion3.model._
import org.scalatest.{BeforeAndAfter, FlatSpec}

class DeDuplicationEnrichmentTest extends FlatSpec with BeforeAndAfter {

  val deDuplicationEnrichment = new DeDuplicationEnrichment

  "enrich" should " remove duplicate Strings" in {
    val originalValue = Seq("foo", "foo")
    val expectedValue = Seq("foo")

    val mappedRecord = MappedRecordsFixture.mappedRecord.copy(
      sourceResource = DplaSourceResource(title = originalValue)
    )

    val expectedRecord= MappedRecordsFixture.mappedRecord.copy(
      sourceResource = DplaSourceResource(title = expectedValue)
    )

    val enrichedRecord = deDuplicationEnrichment.enrich(mappedRecord)

    assert(enrichedRecord === expectedRecord)
  }

  "enrich" should " remove duplicate entities" in {
    def collection = DcmiTypeCollection(title = Some("foo"))
    val originalValue = Seq(collection, collection)
    val expectedValue = Seq(collection)

    val mappedRecord = MappedRecordsFixture.mappedRecord.copy(
      sourceResource = DplaSourceResource(collection = originalValue)
    )

    val expectedRecord= MappedRecordsFixture.mappedRecord.copy(
      sourceResource = DplaSourceResource(collection = expectedValue)
    )

    val enrichedRecord = deDuplicationEnrichment.enrich(mappedRecord)

    assert(enrichedRecord === expectedRecord)
  }
}
