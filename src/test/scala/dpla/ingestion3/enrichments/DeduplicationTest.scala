package dpla.ingestion3.enrichments

import dpla.ingestion3.data.MappedRecordsFixture
import dpla.ingestion3.enrichments.normalizations.Deduplication
import dpla.ingestion3.model._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{BeforeAndAfter}

class DeduplicationTest extends AnyFlatSpec with BeforeAndAfter {

  val deDuplicationEnrichment = new Deduplication

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

  "enrich" should " remove duplicate collection entities" in {
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

  "enrich" should " remove duplicate EdmAgent entities" in {
    def agentA= nameOnlyAgent("Tom Jones")
    def agentB= nameOnlyAgent("Rick Rolls")
    def agentC= nameOnlyAgent("Samwell Gamgee")

    val originalValue = Seq(agentA, agentC, agentA, agentB)
    val expectedValue = Seq(agentA, agentC, agentB)

    val mappedRecord = MappedRecordsFixture.mappedRecord.copy(
      sourceResource = DplaSourceResource(creator = originalValue)
    )

    val expectedRecord = MappedRecordsFixture.mappedRecord.copy(
      sourceResource = DplaSourceResource(creator = expectedValue)
    )

    val enrichedRecord = deDuplicationEnrichment.enrich(mappedRecord)

    assert(enrichedRecord === expectedRecord)
  }
}
