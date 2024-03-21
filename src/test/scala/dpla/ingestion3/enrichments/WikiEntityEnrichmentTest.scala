package dpla.ingestion3.enrichments

import dpla.ingestion3.model.{EdmAgent, URI}
import org.scalatest.BeforeAndAfter
import dpla.ingestion3.model._
import org.scalatest.flatspec.AnyFlatSpec

class WikiEntityEnrichmentTest extends AnyFlatSpec with BeforeAndAfter {

  val wikiEnrichment = new WikiEntityEnrichment

  "WikiEntityEnrichment" should "return an enriched EdmAgent for 'University of Pennsylvania'" in {
    val originalValue = nameOnlyAgent("University of Pennsylvania")
    val qualifierValue = Option(nameOnlyAgent("PA Digital"))
    val expectedValue = EdmAgent(
      name = Some("University of Pennsylvania"),
      exactMatch = Seq(URI("http://www.wikidata.org/entity/Q49117"))
    )
     assert(wikiEnrichment.enrichEntity(originalValue, qualifierValue) === expectedValue)
  }

  it should "return an enriched EdmAgent for 'pa digitaluniversity of Pennsylvania' (case-insensitive)" in {
    val originalValue = nameOnlyAgent("university of Pennsylvania")
    val qualifierValue = nameOnlyAgent("PA Digital")
    val expectedValue = EdmAgent(
      name = Some("university of Pennsylvania"),
      exactMatch = Seq(URI("http://www.wikidata.org/entity/Q49117"))
    )
     assert(wikiEnrichment.enrichEntity(originalValue, Option(qualifierValue)) === expectedValue)
  }

  it should "return an enriched EdmAgent for 'pa digital'" in {
    val originalValue = EdmAgent(
      name = Some("PA Digital"),
      uri = Some(URI("http://dp.la/api/contributor/pa"))
    )
    val expectedValue = EdmAgent(
      name = Some("PA Digital"),
      exactMatch = Seq(URI("http://www.wikidata.org/entity/Q83878501")),
      uri = Some(URI("http://dp.la/api/contributor/pa"))
    )
    assert(wikiEnrichment.enrichEntity(originalValue) === expectedValue)
  }

  it should "return an enriched EdmAgent for 'pa digital' (case-insensitive)" in {
    val originalValue = nameOnlyAgent("pa digital")
    val expectedValue = EdmAgent(
      name = Some("pa digital"),
      exactMatch = Seq(URI("http://www.wikidata.org/entity/Q83878501"))
    )
    assert(wikiEnrichment.enrichEntity(originalValue) === expectedValue)
  }

  it should "return an non-enriched EdmAgent for 'bogus' (case-insensitive)" in {
    val originalValue = nameOnlyAgent("bogus")
    val expectedValue = EdmAgent(
      name = Some("bogus"),
      exactMatch = Seq()
    )
    assert(wikiEnrichment.enrichEntity(originalValue) === expectedValue)
  }

  it should "return an non-enriched EdmAgent for entities without IDs (case-insensitive)" in {
    val qualifiedValue = nameOnlyAgent("Missouri Hub")
    val originalValue = nameOnlyAgent("Springfield - Greene County Library")
    val expectedValue = EdmAgent(
      name = Some("Springfield - Greene County Library"),
      exactMatch = Seq()
    )
    assert(wikiEnrichment.enrichEntity(originalValue, Some(qualifiedValue)) === expectedValue)
  }
}
