package dpla.ingestion3.enrichments

import org.scalatest.{BeforeAndAfter, FlatSpec}

class ExtentFromFormatTest extends FlatSpec with BeforeAndAfter {



  val typeEnrichment = new TypeEnrichment

  "ExtentFromFormatFilter" should "return an enriched string for 'appliance'" in {
    val originalValue = "appliance"
    val expectedValue = Some("physical object")
    assert(typeEnrichment.enrich(originalValue) === expectedValue)
  }
  it should "return an enriched string for 'Image'" in {
    val originalValue = "Image"
    val expectedValue = Some("image")
    assert(typeEnrichment.enrich(originalValue) === expectedValue)
  }
  it should "return None for 'bucket'" in {
    val originalValue = "Bucket"
    val expectedValue = None
    assert(typeEnrichment.enrich(originalValue) === expectedValue)
  }
}
