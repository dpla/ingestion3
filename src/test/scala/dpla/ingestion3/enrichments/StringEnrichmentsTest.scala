package dpla.ingestion3.enrichments

import org.scalatest.{BeforeAndAfter, FlatSpec}

class StringEnrichmentsTest extends FlatSpec with BeforeAndAfter {
  val enrichments = new StringEnrichments

  "convertToSentenceCase " should " capitalize the first character in each " +
    "sentence." in {
    val originalValue = "this is a sentence about Moomins. this is another about Snorks."
    val enrichedValue = enrichments.convertToSentenceCase(originalValue)
    val expectedValue = "This is a sentence about Moomins. This is another about Snorks."

    assert(enrichedValue === expectedValue)
  }

  "splitAtDelimiter" should " split a string around semi-colon by default" in {
    val originalValue = "subject-one; subject-two; subject-three"
    val enrichedValue = enrichments.splitAtDelimiter(originalValue)
    val expectedValue = Array("subject-one", "subject-two", "subject-three")

    assert(enrichedValue === expectedValue)
  }

  "splitAtDelimiter" should " split a string around comma." in {
    val originalValue = "subject-one, subject-two; subject-three"
    val enrichedValue = enrichments.splitAtDelimiter(originalValue, ",")
    val expectedValue = Array("subject-one", "subject-two; subject-three")

    assert(enrichedValue === expectedValue)
  }

}
