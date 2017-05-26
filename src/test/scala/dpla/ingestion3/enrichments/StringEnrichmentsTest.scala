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

  "splitAtSemiColons" should " split a string around semi-colon" in {
    val originalValue = "subject-one; subject-two; subject-three"
    val enrichedValue = enrichments.splitAtSemicolons(originalValue)
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

  "stripPunctuation" should " strip all punctuation from the given string" in {
    val originalValue = "\t\"It's @#$,.! OK\"\n"
    val enrichedValue = enrichments.stripPunctuation(originalValue)
    val expectedValue = "\t\"It's  OK\"\n"
    assert(enrichedValue === expectedValue)
  }

  "stripLeadingPunctuation" should " strip leading punctuation from the " +
      "given string" in {
    val originalValue = "@#$,.!\t \"It's OK\""
    val enrichedValue = enrichments.stripPunctuation(originalValue)
    val expectedValue = "\t \"It's OK\""
    assert(enrichedValue === expectedValue)
  }

  "stripEndingPunctuation" should " strip ending punctuation from the " +
    "given string" in {
    val originalValue = "\"It's OK\" @#$,.!"
    val enrichedValue = enrichments.stripPunctuation(originalValue)
    val expectedValue = "\"It's OK\" "
    assert(enrichedValue === expectedValue)
  }

  "stringWhitespace" should " strip leading, ending and duplicate whitespace" +
    "within a given string" in {
    val originalValue = "  one      two  three  "
    val enrichedValue = enrichments.stripWhitespace(originalValue)
    val expectedValue = "one two three"
    assert(enrichedValue === expectedValue)
  }

  "stripLeadingColons" should " remove ; and : from the start of a given " +
  "String" in {
    val originalValue = ";;:Hello ;world:! ;; "
    val enrichedValue = enrichments.stripLeadingColons(originalValue)
    val expectedValue = "Hello ;world:! ;; "
    assert(enrichedValue === expectedValue)
  }

  it should " not change a String that does not being with ; or :" in {
    val originalValue = "\n.Question: What is best in life?\n" +
      "Answer: Bears, Beets, Battlestar Galactica "
    val enrichedValue = enrichments.stripLeadingColons(originalValue)
    val expectedValue = originalValue
    assert(enrichedValue === expectedValue)
  }

  "limitCharacters" should " truncate a given string to under the given " +
      "length, terminated with ellipses" in {
    val originalValue = "Lorem ipsum"
    val enrichedValue = enrichments.limitCharacters(originalValue, 10)
    val expectedValue = "Lorem..."
    assert(enrichedValue === expectedValue)
  }

  it should "not alter strings that fit within the limit" in {
    val originalValue = "Lorem"
    val enrichedValue = enrichments.limitCharacters(originalValue, 10)
    val expectedValue = "Lorem"
    assert(enrichedValue === expectedValue)
  }

  it should "truncate at whitespace" in {
    val originalValue = "Lorem ipsum dolor"  // 17 characters
    val enrichedValue = enrichments.limitCharacters(originalValue, 16)
    val expectedValue = "Lorem ipsum..."  // 14 characters
    assert(enrichedValue === expectedValue)
  }

  it should "return same string given sillly short length of 3" in {
    val originalValue = "Lorem"
    val enrichedValue = enrichments.limitCharacters(originalValue, 3)
    val expectedValue = "Lor"
    assert(enrichedValue === expectedValue)
  }
}
