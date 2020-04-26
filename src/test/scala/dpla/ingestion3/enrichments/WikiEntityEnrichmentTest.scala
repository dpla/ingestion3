package dpla.ingestion3.enrichments

import dpla.ingestion3.model.{EdmAgent, URI}
import org.scalatest.{BeforeAndAfter, FlatSpec}
import dpla.ingestion3.model._

class WikiEntityEnrichmentTest extends FlatSpec with BeforeAndAfter {

  val wikiEnrichment = new WikiEntityEnrichment

  "WikiEntityEnrichment" should "return an enriched EdmAgent for 'University of Pennsylvania'" in {
    val originalValue = nameOnlyAgent("University of Pennsylvania")
    val expectedValue = EdmAgent(
      name = Some("University of Pennsylvania"),
      exactMatch = Seq(URI("Q49117"))
    )
    assert(wikiEnrichment.enrichEntity(originalValue) === expectedValue)
  }

  it should "return an enriched EdmAgent for 'university of Pennsylvania' (case-insensitive)" in {
    val originalValue = nameOnlyAgent("university of Pennsylvania")
    val expectedValue = EdmAgent(
      name = Some("university of Pennsylvania"),
      exactMatch = Seq(URI("Q49117"))
    )
    assert(wikiEnrichment.enrichEntity(originalValue) === expectedValue)
  }
}
