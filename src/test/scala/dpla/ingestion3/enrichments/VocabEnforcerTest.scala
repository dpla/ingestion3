package dpla.ingestion3.enrichments

import java.net.URI

import dpla.ingestion3.mappers.rdf.DCMIType
import dpla.ingestion3.model.SkosConcept
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

  it should " return an None when given " +
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

  it should " return False when given " +
    "a string that does not exactly match the local name of a DcmiType IRI" in {
    val originalValue = "images"
    val expectedValue = false
    val enrichedValue = DcmiTypeEnforcer.enforceDcmiType(originalValue)

    assert(enrichedValue === expectedValue)
  }

  "Lexvo enrichment " should " an enriched SkosConcept object when given a valid iso-639 " +
    "abbreviation" in {
    val originalValue = Seq(SkosConcept(providedLabel = Option("eng")))
    val expectedValue = Seq(SkosConcept(
      providedLabel = Option("eng"),
      concept = Option("English"),
      scheme = Option(new URI("http://lexvo.org/id/iso639-3/"))
    ))

    val enrichedValue = originalValue.map(LanguageMapper.mapLanguage)

    assert(enrichedValue === expectedValue)
  }

  it should " an enriched SkosConcept object when given a valid iso-639 regardless of case" +
    "abbreviation" in {
    val originalValue = Seq(SkosConcept(providedLabel = Option("ENG")))
    val expectedValue = Seq(SkosConcept(
      providedLabel = Option("ENG"),
      concept = Option("English"),
      scheme = Option(new URI("http://lexvo.org/id/iso639-3/"))
    ))

    val enrichedValue = originalValue.map(LanguageMapper.mapLanguage)

    assert(enrichedValue === expectedValue)
  }

  it should "return an unenriched SkosConcept object when given a invalid iso-639 " +
    "abbreviation. E.g. the return value should match the original value" in {
    val originalValue = SkosConcept(providedLabel = Option("b__"))
    val expectedValue = SkosConcept(providedLabel = Option("b__"))

    val enrichedValue = LanguageMapper.mapLanguage(originalValue)

    assert(enrichedValue === expectedValue)
  }
}


