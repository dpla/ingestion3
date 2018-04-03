package dpla.ingestion3.enrichments

import org.scalatest.{BeforeAndAfter, FlatSpec}

class TypeMapperTest  extends FlatSpec with BeforeAndAfter {

  val typeMapper = new TypeMapper

  "Type mapper" should "return an enriched string for 'appliance'" in {
    val originalValue = "appliance"
    val expectedValue = Some("image")
    assert(typeMapper.enrich(originalValue) === expectedValue)
  }
  it should "return an enriched string for 'Image'" in {
    val originalValue = "Image"
    val expectedValue = Some("image")
    assert(typeMapper.enrich(originalValue) === expectedValue)
  }
  it should "return None for 'bucket'" in {
    val originalValue = "Bucket"
    val expectedValue = None
    assert(typeMapper.enrich(originalValue) === expectedValue)
  }
}
