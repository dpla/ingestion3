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

  "cleanupLeadingPunctuation" should "strip leading punctuation from a string" in {
    val originalValue = ":  ;; --  It's @@ OK --- "
    val enrichedValue = originalValue.cleanupLeadingPunctuation
    val expectedValue = "It's @@ OK --- "
    assert(enrichedValue === expectedValue)
  }

  it should "remove whitespace" in {
    val originalValue = "   A good string "
    val enrichedValue = originalValue.cleanupLeadingPunctuation
    val expectedValue = "A good string "
    assert(enrichedValue === expectedValue)
  }

  it should "remove tabs" in {
    val originalValue = "\t\t\tA \tgood string "
    val enrichedValue = originalValue.cleanupLeadingPunctuation
    val expectedValue = "A \tgood string "
    assert(enrichedValue === expectedValue)
  }

  it should "remove new line characters" in {
    val originalValue = "\n\n\r\nA good string "
    val enrichedValue = originalValue.cleanupLeadingPunctuation
    val expectedValue = "A good string "
    assert(enrichedValue === expectedValue)
  }

  it should "do nothing if there is no punctuation" in {
    val originalValue = "A good string "
    val enrichedValue = originalValue.cleanupLeadingPunctuation
    val expectedValue = "A good string "
    assert(enrichedValue === expectedValue)
  }

  "cleanupEndingPunctuation" should "strip punctuation following the last letter or digit character" in {
    val originalValue = ".. It's OK  ;; .. ,, // \n"
    val enrichedValue = originalValue.cleanupEndingPunctuation
    val expectedValue = ".. It's OK"
    assert(enrichedValue === expectedValue)
  }

  it should "remove whitespace" in {
    val originalValue = "A good string   "
    val enrichedValue = originalValue.cleanupEndingPunctuation
    val expectedValue = "A good string"
    assert(enrichedValue === expectedValue)
  }

  it should "remove tabs" in {
    val originalValue = "A \tgood string\t\t\t"
    val enrichedValue = originalValue.cleanupEndingPunctuation
    val expectedValue = "A \tgood string"
    assert(enrichedValue === expectedValue)
  }

  it should "remove new line characters" in {
    val originalValue = "A good string\n\n\r\n"
    val enrichedValue = originalValue.cleanupEndingPunctuation
    val expectedValue = "A good string"
    assert(enrichedValue === expectedValue)
  }

  it should "do nothing if there is no ending punctuation" in {
    val originalValue = "A good string"
    val enrichedValue = originalValue.cleanupEndingPunctuation
    val expectedValue = "A good string"
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
  it should "not capitalize anything in a string with alphanumeric characters" in {
    val originalValue = "...@..|}"
    val enrichedValue = originalValue.capitalizeFirstChar
    assert(enrichedValue === "...@..|}")
  }
  it should "not capitalize anything in an empty string" in {
    val originalValue = ""
    val enrichedValue = originalValue.capitalizeFirstChar
    assert(enrichedValue === "")
  }

  "findAndRemoveAll" should "remove all stop words from the given string" in {
    val stopWords = Set("jp2", "application/xml")
    val originalValue = "application/xml photograph   jp2"
    val enrichedValue = originalValue.findAndRemoveAll(stopWords)
    assert(enrichedValue === "photograph")
  }
}
