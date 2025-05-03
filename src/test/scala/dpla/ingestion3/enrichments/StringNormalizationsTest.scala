package dpla.ingestion3.enrichments

import dpla.ingestion3.data.MappedRecordsFixture
import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils.Normalizations
import dpla.ingestion3.enrichments.normalizations.StringNormalizations
import dpla.ingestion3.model._
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class StringNormalizationsTest extends AnyFlatSpec with BeforeAndAfter {

  val stringNormalizations = new StringNormalizations

  "enrich" should "reduce duplicate whitespace" in {
    val originalString = "foo  bar"
    val expectedString = "foo bar"

    val mappedRecord = MappedRecordsFixture.mappedRecord.copy(
      sourceResource = DplaSourceResource(title = Seq(originalString))
    )

    val expectedRecord= MappedRecordsFixture.mappedRecord.copy(
      sourceResource = DplaSourceResource(title = Seq(expectedString))
    )

    val enrichedRecord = stringNormalizations.enrich(mappedRecord)

    assert(enrichedRecord === expectedRecord)
  }

  it should "remove HTML markup" in {
    val expectedString = "foo bar baz buzz"
    val originalString = f"<p>$expectedString%s</p>"

    val mappedRecord = MappedRecordsFixture.mappedRecord.copy(
      sourceResource = DplaSourceResource(title = Seq(originalString))
    )

    val expectedRecord= MappedRecordsFixture.mappedRecord.copy(
      sourceResource = DplaSourceResource(title = Seq(expectedString))
    )

    val enrichedRecord = stringNormalizations.enrich(mappedRecord)

    assert(enrichedRecord === expectedRecord)
  }

  it should "remove trailing whitespace from edmAgent.name" in {
    val agent = Seq("Indiana Harbor Belt Railroad Company ").map(nameOnlyAgent)
    val expectedAgent = Seq("Indiana Harbor Belt Railroad Company").map(nameOnlyAgent)

    val mappedRecord = MappedRecordsFixture.mappedRecord.copy(
      sourceResource = DplaSourceResource(creator = agent)
    )

    val expectedRecord= MappedRecordsFixture.mappedRecord.copy(
      sourceResource = DplaSourceResource(creator = expectedAgent)
    )

    val enrichedRecord = stringNormalizations.enrich(mappedRecord)

    assert(enrichedRecord === expectedRecord)
  }

  it should "not throw up on strings that look like regular expressions" in {
    val evilString = "[H] __[W] 77.47 cm (top 25.4 cm &quot; __[D] __[Diam] __[L] 139.7 cm __[Remarks]"
    val termList =  Set(
      "^.*[^a-zA-Z]\\s*x\\s*[^a-zA-Z].*$", // 1 x 2 OR 1 X 2 OR 1x2 but not 1 xerox
      "^[0-9].*" // starts with number
    )
    //mos
    assert(evilString.applyAllowFilter(termList) === "")
  }
}
