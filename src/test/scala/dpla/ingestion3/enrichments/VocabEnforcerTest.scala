package dpla.ingestion3.enrichments

import dpla.ingestion3.mappers.rdf.DCMIType
import org.scalatest.{BeforeAndAfter, FlatSpec}

class VocabEnforcerTest extends FlatSpec with BeforeAndAfter {

  val dcmiType = DCMIType()

  "DcmiTypeMapper.mapDcmiType" should " return an Option[IRI] when given " +
    "a valid string (e.g. image, costume, moving image)" in {
    val originalValue = "image"
    val expectedValue = Some(dcmiType.Image)
    val enrichedValue = DcmiTypeMapper.mapDcmiType(originalValue)

    assert(enrichedValue === expectedValue)
  }

  "DcmiTypeMapper.mapDcmiType" should " return an None when given " +
    "an invalid string (e.g. death star, cleganebowl)" in {
    val originalValue = "cleganebowl"
    val expectedValue = None
    val enrichedValue = DcmiTypeMapper.mapDcmiType(originalValue)

    assert(enrichedValue === expectedValue)
  }

  "enforceDcmiType" should " return True when given " +
    "a string that exactly matches the local name of a Dcmi type IRI" in {
    val originalValue = "Image"
    val expectedValue = true
    val enrichedValue = DcmiTypeEnforcer.enforceDcmiType(originalValue)

    assert(enrichedValue === expectedValue)
  }

  "enforceDcmiType" should " return False when given " +
    "a string that does not exactly match the local name of a DcmiType IRI" in {
    val originalValue = "images"
    val expectedValue = false
    val enrichedValue = DcmiTypeEnforcer.enforceDcmiType(originalValue)

    assert(enrichedValue === expectedValue)
  }
}


