package dpla.ingestion3.enrichments

import org.scalatest.{BeforeAndAfter, FlatSpec}
import dpla.ingestion3.enrichments.StringUtils._
import dpla.ingestion3.enrichments.filters.DigitalSurrogateBlockList
import dpla.ingestion3.enrichments.FilterRegex._

class StringUtilsTest extends FlatSpec with BeforeAndAfter {

  // Helper objects
  object BlockList extends FilterList {
    override val termList: Set[String] = Set(
      "jpeg",
      "jpeg/2000",
      "tiff",
      "bitmap image",
      "application+pdf"
    ).map(_.blockListRegex) // TODO Should this term list be testing the blockListRegex or just literal term?
  }

  object AllowList extends FilterList {
    override val termList: Set[String] = Set(
      "moving image",
      "film",
      "audio",
      "image"
    ).map(_.blockListRegex) // TODO Should this term list be testing the blockListRegex or just literal term?
  }

  // Tests
  "convertToSentenceCase" should "capitalize the first character in each sentence" in {
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

  it should "remove unbalanced and invalid html from a given string" in {
    val expectedValue = "foo bar baz buzz"
    val originalValue = f"<p>$expectedValue%s</i><html>"
    val enrichedValue = originalValue.stripHTML
    assert(enrichedValue === expectedValue)
  }

  it should "not modify strings that do not contain html markup" in {
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

  it should "reduce five whitespaces to one whitespace" in {
    val originalValue = "foo     bar"
    val enrichedValue = originalValue.reduceWhitespace
    assert(enrichedValue === "foo bar")
  }

  it should "reduce multiple occurrences duplicate whitespace to single whitespace" in {
    val originalValue = "foo   bar  choo"
    val enrichedValue = originalValue.reduceWhitespace
    assert(enrichedValue === "foo bar choo")
  }
  it should "reduce remove leading and trailing white space" in {
    val originalValue = "   foo bar  choo "
    val enrichedValue = originalValue.reduceWhitespace
    assert(enrichedValue === "foo bar choo")
  }

  "capitalizeFirstChar" should "not capitalize the b in '3 blind mice'" in {
    val originalValue = "3 blind mice"
    val enrichedValue = originalValue.capitalizeFirstChar
    assert(enrichedValue === "3 blind mice")
  }
  it should "capitalize the t in 'three blind mice'" in {
    val originalValue = "three blind mice"
    val enrichedValue = originalValue.capitalizeFirstChar
    assert(enrichedValue === "Three blind mice")
  }
  it should "capitalize the v in '...vacationland...'" in {
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

  /**
    * applyBlockFilter tests
    * @see FilterRegexTest blockListRegex tests
    */
  "applyBlockFilter" should "remove a block term" in {
    val originalValue = "jpeg"
    val enrichedValue = originalValue.applyBlockFilter(BlockList.termList)
    assert(enrichedValue === "")
  }
  it should "remove a block term with extra white space" in {
    val originalValue = "  jpeg  "
    val enrichedValue = originalValue.applyBlockFilter(BlockList.termList)
    assert(enrichedValue === "")
  }
  it should "not remove a block term if it is not an exact match" in {
    val originalValue = "jpeg photo"
    val enrichedValue = originalValue.applyBlockFilter(BlockList.termList)
    assert(enrichedValue === "jpeg photo")
  }
  /**
    * applyAllowFilter
    *
    *
    */

  "applyAllowFilter" should "retain only the allowed term ('moving image' in 'moving image'" in {
    val originalValue = "moving image"
    val enrichedValue = originalValue.applyAllowFilter(AllowList.termList)
    assert(enrichedValue === "moving image")
  }
  it should "remove a term that is not on the allow list" in {
    val originalValue = "dvd"
    val enrichedValue = originalValue.applyAllowFilter(AllowList.termList)
    assert(enrichedValue === "")
  }
  it should "ignore extraneous white space ('  moving image  ' remains 'moving image')" in {
    val originalValue = " moving image      "
    val enrichedValue = originalValue.applyAllowFilter(AllowList.termList)
    assert(enrichedValue === "moving image")
  }
  it should "match regardless of case ('MOVING image')" in {
    val originalValue = "MOVING image"
    val enrichedValue = originalValue.applyAllowFilter(AllowList.termList)
    assert(enrichedValue === "MOVING image")
  }

//  TODO Term filtering within string (ignore commas) not yet supported
//  it should "ignore commas and retain all allowed terms ('moving image, film' remains 'moving image, film')" in {
//    val originalValue = "moving image, image"
//    val enrichedValue = originalValue.applyAllowFilter(AllowList.termList)
//    assert(enrichedValue === "moving image, image")
//  }
  //  TODO Term filtering within string not yet supported
//  it should "retain all allowed words ('moving image film' in'moving image dvd film')" in {
//    val originalValue = "moving image dvd film"
//    val enrichedValue = originalValue.applyAllowFilter(AllowList.termList)
//    assert(enrichedValue === "moving image film")
//  }
//  TODO Term filtering within string (extra whitespace) not yet supported
//  it should "ignore extraneous white space and remove non-allowed terms " +
//    "('  moving image    dvd  ' removes 'dvd')" in {
//    val originalValue = " moving image    dvd   "
//    val enrichedValue = originalValue.applyAllowFilter(AllowList.termList)
//    assert(enrichedValue === "moving image")
//  }

  /**
    * Strip Brackets
    */
  "stripBrackets" should "remove leading and trailing ( )" in {
    val originalValue = "(hello)"
    val enrichedValue = originalValue.stripBrackets
    assert(enrichedValue === "hello")
  }
  it should "remove leading and trailing [ ]" in {
    val originalValue = "[hello]"
    val enrichedValue = originalValue.stripBrackets
    assert(enrichedValue === "hello")
  }
  it should "remove leading and trailing { }" in {
    val originalValue = "{hello}"
    val enrichedValue = originalValue.stripBrackets
    assert(enrichedValue === "hello")
  }
  it should "ignore whitespace and remove leading and trailing { } " in {
    val originalValue = " \t{hello} \n"
    val enrichedValue = originalValue.stripBrackets
    assert(enrichedValue === "hello")
  }
  it should "leave interior brackets alone" in {
    val originalValue = "Hello ()[]{} Goodbye"
    val enrichedValue = originalValue.stripBrackets
    assert(enrichedValue === "Hello ()[]{} Goodbye")
  }
  it should "remove surrounding brackets and interior brackets alone" in {
    val originalValue = "( {Hello ()[]{} Goodbye)"
    val enrichedValue = originalValue.stripBrackets
    assert(enrichedValue === "{Hello ()[]{} Goodbye")
  }
  it should "do nothing with unmatched brackets" in {
    val originalValue = "(Hello"
    val enrichedValue = originalValue.stripBrackets
    assert(enrichedValue === "(Hello")
  }

  "stripEndingPeriod" should "remove a single trailing period" in {
    val originalValue = "Hello."
    val enrichedValue = originalValue.stripEndingPeriod
    val expectedValue = "Hello"
    assert(enrichedValue === expectedValue)
  }
  it should "not remove ellipsis" in {
    val originalValue = "Hello..."
    val enrichedValue = originalValue.stripEndingPeriod
    val expectedValue = "Hello..."
    assert(enrichedValue === expectedValue)
  }
  it should "not remove leading or interior periods" in {
    val originalValue = "H.e.l.l.o."
    val enrichedValue = originalValue.stripEndingPeriod
    val expectedValue = "H.e.l.l.o"
    assert(enrichedValue === expectedValue)
  }
  it should "return the original value if only given a single period (e.g. '.')" in {
    val originalValue = "."
    val enrichedValue = originalValue.stripEndingPeriod
    val expectedValue = "."
    assert(enrichedValue === expectedValue)
  }
  it should "remove a trailing period if it followed by whitespace" in {
    val originalValue = "Hello.  "
    val enrichedValue = originalValue.stripEndingPeriod
    val expectedValue = "Hello"
    assert(enrichedValue === expectedValue)
  }
}
