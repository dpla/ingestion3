package dpla.ingestion3.enrichments

import dpla.ingestion3.model.SkosConcept
import org.scalatest.{BeforeAndAfter, FlatSpec}

class LanguageMapperTest extends FlatSpec with BeforeAndAfter {
  "Language mapper" should "return an enriched SkosConcept object when given a valid iso-639 code" in {
    val originalValue = SkosConcept(
      providedLabel = Some("eng")
    )
    val expectedValue = SkosConcept(
      providedLabel = Some("eng"),
      concept = Some("English")
    )

    val enrichedValue = LanguageMapper.enrich(originalValue)
    assert(enrichedValue === expectedValue)
  }
  it should "return an unenriched SkosConcept object when given a invalid iso-639 " +
    "abbreviation. E.g. the return value should match the original value" in {
    val originalValue = SkosConcept(providedLabel = Option("b__"))
    val expectedValue = SkosConcept(providedLabel = Option("b__"))

    val enrichedValue = LanguageMapper.enrich(originalValue)

    assert(enrichedValue === expectedValue)
  }
  it should "return an enriched SkosConcept object when given a DPLA specific code" +
    "(en us)" in {
    val originalValue = SkosConcept(providedLabel = Option("en us"))
    val expectedValue = SkosConcept(
      providedLabel = Option("en us"),
      concept = Option("English")
    )

    val enrichedValue = LanguageMapper.enrich(originalValue)

    assert(enrichedValue === expectedValue)
  }
}
