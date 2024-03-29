package dpla.ingestion3.enrichments

import dpla.ingestion3.model.SkosConcept
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class LanguageEnrichmentTest extends AnyFlatSpec with BeforeAndAfter {

  val languageEnrichment = new LanguageEnrichment

  "Language mapper" should "return an enriched SkosConcept for 'eng'" in {
    val originalValue = SkosConcept(
      providedLabel = Some("eng")
    )
    val expectedValue = SkosConcept(
      providedLabel = Some("eng"),
      concept = Some("English")
    )
    assert(languageEnrichment.enrichLanguage(originalValue) === expectedValue)
  }
  it should "return an enriched SkosConcept for 'en'" in {
    val originalValue = SkosConcept(
      providedLabel = Some("en")
    )
    val expectedValue = SkosConcept(
      providedLabel = Some("en"),
      concept = Some("English")
    )
    assert(languageEnrichment.enrichLanguage(originalValue) === expectedValue)
  }
  it should "return the original SkosConcept when given a invalid language code" in {
    val originalValue = SkosConcept(providedLabel = Option("b__"))
    assert(languageEnrichment.enrichLanguage(originalValue) === originalValue)
  }
  it should "return an enriched SkosConcept for 'en us'" +
    "(en us)" in {
    val originalValue = SkosConcept(providedLabel = Option("en us"))
    val expectedValue = SkosConcept(
      providedLabel = Option("en us"),
      concept = Option("English")
    )
    assert(languageEnrichment.enrichLanguage(originalValue) === expectedValue)
  }
  it should "return an enriched SkosConcept for 'ENG')" in {
    val originalValue = SkosConcept(providedLabel = Option("ENG"))
    val expectedValue = SkosConcept(
      providedLabel = Option("ENG"),
      concept = Option("English")
    )
    assert(languageEnrichment.enrichLanguage(originalValue) === expectedValue)
  }
  it should "return an enriched SkosConcept for ' ENG ')" in {
    val originalValue = SkosConcept(providedLabel = Option("ENG"))
    val expectedValue = SkosConcept(
      providedLabel = Option("ENG"),
      concept = Option("English")
    )
    assert(languageEnrichment.enrichLanguage(originalValue) === expectedValue)
  }
  it should "return an enriched SkosConcept for 'English')" in {
    val originalValue = SkosConcept(providedLabel = Option("English"))
    val expectedValue = SkosConcept(
      providedLabel = Option("English"),
      concept = Option("English")
    )
    assert(languageEnrichment.enrich(originalValue) === Option(expectedValue))
  }
  //gre,"Greek, Modern (1453-)"
  it should "return an enriched SkosConcept for 'gre')" in {
    val originalValue = SkosConcept(providedLabel = Option("gre"))
    val expectedValue = SkosConcept(
      providedLabel = Option("gre"),
      concept = Option("Greek, Modern (1453-)")
    )
    assert(languageEnrichment.enrich(originalValue) === Option(expectedValue))
  }
  //spa,Spanish
  it should "return an enriched SkosConcept for 'spa')" in {
    val originalValue = SkosConcept(providedLabel = Option("spa"))
    val expectedValue = SkosConcept(
      providedLabel = Option("spa"),
      concept = Option("Spanish")
    )
    assert(languageEnrichment.enrich(originalValue) === Option(expectedValue))
  }
}
