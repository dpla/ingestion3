package dpla.ingestion3.enrichments

import org.scalatest.{BeforeAndAfter, FlatSpec}
import dpla.ingestion3.enrichments.StringUtils._

class StringUtilsTest extends FlatSpec with BeforeAndAfter {

  "convertToSentenceCase" should "capitalize the first character in each " +
    "sentence." in {
    val originalValue = "this is a sentence about Moomins. this is another about Snorks."
    val enrichedValue = originalValue.convertToSentenceCase
    val expectedValue = "This is a sentence about Moomins. This is another about Snorks."

    assert(enrichedValue === expectedValue)
  }

  "splitAtDelimiter" should "split a string around semi-colon" in {
    val originalValue = "subject-one; subject-two; subject-three"
    val enrichedValue = originalValue.splitAtDelimiter(";")
    val expectedValue = Array("subject-one", "subject-two", "subject-three")

    assert(enrichedValue === expectedValue)
  }

  "splitAtDelimiter" should "drop empty values" in {
    val originalValue = "subject-one; ; subject-three"
    val enrichedValue = originalValue.splitAtDelimiter(";")
    val expectedValue = Array("subject-one", "subject-three")

    assert(enrichedValue === expectedValue)
  }

  "splitAtDelimiter" should "split a string around comma." in {
    val originalValue = "subject-one, subject-two; subject-three"
    val enrichedValue = originalValue.splitAtDelimiter(",")
    val expectedValue = Array("subject-one", "subject-two; subject-three")

    assert(enrichedValue === expectedValue)
  }

  "stripHMTL" should "remove html from a string" in {
    val expectedValue = "foo bar baz buzz"
    val originalValue = f"<p>$expectedValue%s</p>"
    val enrichedValue = originalValue.stripHTML
    assert(enrichedValue === expectedValue)
  }

  it should "handle strings with unbalanced and invalid html" in {
    val expectedValue = "foo bar baz buzz"
    val originalValue = f"<p>$expectedValue%s</i><html>"
    val enrichedValue = originalValue.stripHTML
    assert(enrichedValue === expectedValue)
  }

  it should "passthrough strings that do not contain html" in {
    val expectedValue = "foo bar baz buzz"
    val originalValue = expectedValue
    val enrichedValue = originalValue.stripHTML
    assert(enrichedValue === expectedValue)
  }

  it should "not emit HTML entities" in {
    val expectedValue = "foo bar baz > buzz"
    val originalValue = expectedValue
    val enrichedValue = originalValue.stripHTML
    assert(enrichedValue === expectedValue)
  }

  it should "not turn html entities into html" in {
    val originalValue = "foo bar baz &lt;p&gt; buzz"
    val expectedValue = "foo bar baz  buzz"
    val enrichedValue = originalValue.stripHTML
    assert(enrichedValue === expectedValue)
  }

  "stripPunctuation" should "strip all punctuation from the given string" in {
    val originalValue = "\t\"It's @#$,.! OK\"\n"
    val enrichedValue = originalValue.stripPunctuation
    val expectedValue = "\t\"It's  OK\"\n"
    assert(enrichedValue === expectedValue)
  }

  "stripLeadingPunctuation" should "strip leading punctuation from the " +
      "given string" in {
    val originalValue = "@#$,.!\t \"It's OK\""
    val enrichedValue = originalValue.stripPunctuation
    val expectedValue = "\t \"It's OK\""
    assert(enrichedValue === expectedValue)
  }

  "stripEndingPunctuation" should "strip ending punctuation from the " +
    "given string" in {
    val originalValue = "\"It's OK\" @#$,.!"
    val enrichedValue = originalValue.stripPunctuation
    val expectedValue = "\"It's OK\" "
    assert(enrichedValue === expectedValue)
  }

  "limitCharacters" should "limit the number of characters in long strings" in {
    val longString = "Now is the time for all good people to come to the aid of the party."
    val enrichedValue = longString.limitCharacters(10)
    assert(enrichedValue.size === 10)
  }

  it should "not limit strings shorter or equal to the limit" in {
    val shortString = "Now is the time"
    val enrichedValue = shortString.limitCharacters(shortString.length)
    assert(enrichedValue.size === shortString.length)
  }

  "reduceWhitespace" should "reduce two whitespaces to one whitespace" in {
    val originalValue = "foo  bar"
    val enrichedValue = originalValue.reduceWhitespace
    assert(enrichedValue === "foo bar")
  }

  "capitalizeFirstChar" should "not capitalize the b in 3 blind mice because it is preceded by 3" in {
    val originalValue = "3 blind mice"
    val enrichedValue = originalValue.capitalizeFirstChar
    assert(enrichedValue === "3 blind mice")
  }

  it should "capitalize the t in three blind mice" in {
    val originalValue = "three blind mice"
    val enrichedValue = originalValue.capitalizeFirstChar
    assert(enrichedValue === "Three blind mice")
  }

  it should "capitalize the v in ...vacationland..." in {
    val originalValue = "...vacationland..."
    val enrichedValue = originalValue.capitalizeFirstChar
    assert(enrichedValue === "...Vacationland...")
  }

  it should "capitalize the t in '  telephone'" in {
    val originalValue = "  telephone"
    val enrichedValue = originalValue.capitalizeFirstChar
    assert(enrichedValue === "  Telephone")
  }

  "findAndRemoveAll" should "remove all stop words from the given string" in {
    val stopWords = Set("jp2", "application/xml")
    val originalValue = "application/xml photograph   jp2"
    val enrichedValue = originalValue.findAndRemoveAll(stopWords)
    assert(enrichedValue === "photograph")
  }
}
