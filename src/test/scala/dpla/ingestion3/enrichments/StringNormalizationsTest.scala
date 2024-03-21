package dpla.ingestion3.enrichments

import dpla.ingestion3.data.MappedRecordsFixture
import dpla.ingestion3.enrichments.normalizations.StringNormalizations
import dpla.ingestion3.model._
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class StringNormalizationsTest extends AnyFlatSpec with BeforeAndAfter {

  val stringEnrichments = new StringNormalizations

  "enrich" should " reduce duplicate whitespace" in {
    val originalString = "foo  bar"
    val expectedString = "foo bar"

    val mappedRecord = MappedRecordsFixture.mappedRecord.copy(
      sourceResource = DplaSourceResource(title = Seq(originalString))
    )

    val expectedRecord= MappedRecordsFixture.mappedRecord.copy(
      sourceResource = DplaSourceResource(title = Seq(expectedString))
    )

    val enrichedRecord = stringEnrichments.enrich(mappedRecord)

    assert(enrichedRecord === expectedRecord)
  }

  "enrich" should " remove HTML markup" in {
    val expectedString = "foo bar baz buzz"
    val originalString = f"<p>$expectedString%s</p>"

    val mappedRecord = MappedRecordsFixture.mappedRecord.copy(
      sourceResource = DplaSourceResource(title = Seq(originalString))
    )

    val expectedRecord= MappedRecordsFixture.mappedRecord.copy(
      sourceResource = DplaSourceResource(title = Seq(expectedString))
    )

    val enrichedRecord = stringEnrichments.enrich(mappedRecord)

    assert(enrichedRecord === expectedRecord)
  }

  it should " remove trailing whitespace from edmAgent.name" in {
    val agent = Seq("Indiana Harbor Belt Railroad Company ").map(nameOnlyAgent)
    val expectedAgent = Seq("Indiana Harbor Belt Railroad Company").map(nameOnlyAgent)

    val mappedRecord = MappedRecordsFixture.mappedRecord.copy(
      sourceResource = DplaSourceResource(creator = agent)
    )

    val expectedRecord= MappedRecordsFixture.mappedRecord.copy(
      sourceResource = DplaSourceResource(creator = expectedAgent)
    )

    val enrichedRecord = stringEnrichments.enrich(mappedRecord)

    assert(enrichedRecord === expectedRecord)
  }
}
