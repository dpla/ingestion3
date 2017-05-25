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

  "stripHMTL" should "remove html from a string" in {
    val expectedValue = "foo bar baz buzz"
    val originalValue = f"<p>$expectedValue%s</p>"
    val enrichedValue = enrichments.stripHTML(originalValue)
    assert(enrichedValue === expectedValue)
  }

  it should "handle strings with unbalanced and invalid html" in {
    val expectedValue = "foo bar baz buzz"
    val originalValue = f"<p>$expectedValue%s</i><html>"
    val enrichedValue = enrichments.stripHTML(originalValue)
    assert(enrichedValue === expectedValue)
  }

  it should "passthrough strings that do not contain html" in {
    val expectedValue = "foo bar baz buzz"
    val originalValue = expectedValue
    val enrichedValue = enrichments.stripHTML(originalValue)
    assert(enrichedValue === expectedValue)
  }

  it should "not emit HTML entities" in {
    val expectedValue = "foo bar baz > buzz"
    val originalValue = expectedValue
    val enrichedValue = enrichments.stripHTML(originalValue)
    assert(enrichedValue === expectedValue)
  }

  it should "not turn html entities into html" in {
    val originalValue = "foo bar baz &lt;p&gt; buzz"
    val expectedValue = "foo bar baz  buzz"
    val enrichedValue = enrichments.stripHTML(originalValue)
    assert(enrichedValue === expectedValue)
  }
}
