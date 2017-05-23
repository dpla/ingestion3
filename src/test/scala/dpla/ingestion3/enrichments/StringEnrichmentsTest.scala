package dpla.ingestion3.enrichments

import org.scalatest.{BeforeAndAfter, FlatSpec}

class StringEnrichmentsTest extends FlatSpec with BeforeAndAfter {
  val enrichments = new StringEnrichments

  "convertToSentenceCase " should " capitalize the first character in each " +
    "sentence." in {
    val originalValue = "this is a sentence about Moomins. this is another about Snorks."
    val enrichValue = enrichments.convertToSentenceCase(originalValue)
    val expectedValue = "This is a sentence about Moomins. This is another about Snorks."

    assert(enrichValue === expectedValue)
  }

}
