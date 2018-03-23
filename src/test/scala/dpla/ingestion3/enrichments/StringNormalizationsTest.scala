package dpla.ingestion3.enrichments

import dpla.ingestion3.data.MappedRecordsFixture
import dpla.ingestion3.enrichments.normalizations.StringNormalizations
import dpla.ingestion3.model._
import org.scalatest.{BeforeAndAfter, FlatSpec}

class StringNormalizationsTest extends FlatSpec with BeforeAndAfter {

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
}
