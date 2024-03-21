package dpla.ingestion3.enrichments

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class TypeEnrichmentTest  extends AnyFlatSpec with BeforeAndAfter {

  val typeEnrichment = new TypeEnrichment

  "Type mapper" should "return an enriched string for 'appliance'" in {
    val originalValue = "appliance"
    val expectedValue = Some("physical object")
    assert(typeEnrichment.enrich(originalValue) === expectedValue)
  }
  it should "return an enriched string for 'Image'" in {
    val originalValue = "Image"
    val expectedValue = Some("image")
    assert(typeEnrichment.enrich(originalValue) === expectedValue)
  }
  it should "return an enriched string for 'book'" in {
    val originalValue = "book"
    val expectedValue = Some("text")
    assert(typeEnrichment.enrich(originalValue) === expectedValue)
  }
  it should "return None for 'bucket'" in {
    val originalValue = "Bucket"
    val expectedValue = None
    assert(typeEnrichment.enrich(originalValue) === expectedValue)
  }
}
