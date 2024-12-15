package dpla.ingestion3.enrichments

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.enrichments.normalizations.FilterList
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class StringNormalizationUtilsTest extends AnyFlatSpec with BeforeAndAfter {

  // Helper objects
  object BlockList extends FilterList {
    override val termList: Set[String] = Set(
      "jpeg",
      "jpeg/2000",
      "tiff",
      "bitmap image",
      "application+pdf"
    )
  }

  object AllowList extends FilterList {
    override val termList: Set[String] = Set(
      "moving image",
      "film",
      "audio",
      "image"
    )
  }

  // Tests
  "cleanupGeocoordinates" should "strip out N and W" in {
    val originalValue = "35.58343N, 83.50822W"
    val enrichedValue = originalValue.cleanupGeocoordinates
    val expectedValue = "35.58343, 83.50822"
    assert(enrichedValue === expectedValue)
  }

  it should "erase when W and N in wrong order" in {
    val originalValue = "35.58343W, 83.50822N"
    val enrichedValue = originalValue.cleanupGeocoordinates
    val expectedValue = ""
    assert(enrichedValue === expectedValue)
  }

  it should "strip out N and W when alone" in {
    val originalValue = "N, W"
    val enrichedValue = originalValue.cleanupGeocoordinates
    val expectedValue = ""
    assert(enrichedValue === expectedValue)
  }

  it should "pass through coordinates without cardinal directions" in {
    val originalValue = "35.58343, 83.50822"
    val enrichedValue = originalValue.cleanupGeocoordinates
    val expectedValue = "35.58343, 83.50822"
    assert(enrichedValue === expectedValue)
  }

  it should "not passthrough craziness" in {
    val originalValue = "pork chop sandwiches"
    val enrichedValue = originalValue.cleanupGeocoordinates
    val expectedValue = ""
    assert(enrichedValue === expectedValue)
  }

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

  it should "drop empty values" in {
    val originalValue = "subject-one; ; subject-three"
    val enrichedValue = originalValue.splitAtDelimiter(";")
    val expectedValue = Array("subject-one", "subject-three")
    assert(enrichedValue === expectedValue)
  }

  it should "split a string around comma." in {
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
    val expectedValue = ".. It's OK.."
    assert(enrichedValue === expectedValue)
  }

  it should "remove whitespace and \n " in {
    val originalValue = "\n    President George W. Bush and Mrs. Laura Bush Stand with President Nicolas Sarkozy of France on the North Portico of the White House After His Arrival for Dinner \n  "
    val enrichedValue = originalValue.cleanupEndingPunctuation
    val expectedValue = "\n    President George W. Bush and Mrs. Laura Bush Stand with President Nicolas Sarkozy of France on the North Portico of the White House After His Arrival for Dinner"
    assert(enrichedValue === expectedValue)
  }


  it should "not remove .) from Synagogues -- Washington (D.C.)" in {
    val originalValue = "Synagogues -- Washington (D.C.)"
    val enrichedValue = originalValue.cleanupEndingPunctuation
    val expectedValue = "Synagogues -- Washington (D.C.)"
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
    assert(enrichedValue.length === 10)
  }

  it should "not limit strings shorter or equal to the limit" in {
    val shortString = "Now is the time"
    val enrichedValue = shortString.limitCharacters(shortString.length)
    assert(enrichedValue.length === shortString.length)
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

  it should "reduce non printable characters" in {
    val originalValue = "   foo bar\u00a0 choo "
    val enrichedValue = originalValue.reduceWhitespace
    assert(enrichedValue === "foo bar choo")
  }

  it should "reduce non printable vertical whitespace characters" in {
    val originalValue = " \u2028foo \u2028  bar\u00a0\u2028 choo "
    val enrichedValue = originalValue.reduceWhitespace
    assert(enrichedValue === "foo bar choo")
  }

  it should "reduce leading and trailing non printable horizontal and vertical whitespace characters" in {
    val originalValue = "\u2028foo choo\u00a0\u2028 "
    val enrichedValue = originalValue.reduceWhitespace
    assert(enrichedValue === "foo choo")
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

  "applyBlockFilter" should "remove a block term" in {
    val originalValue = "jpeg"
    val enrichedValue = originalValue.applyBlockFilter(BlockList.termList)
    assert(enrichedValue === "")
  }

  it should "remove a block term if surrounded by extra white space" in {
    val originalValue = "  jpeg  "
    val enrichedValue = originalValue.applyBlockFilter(BlockList.termList)
    assert(enrichedValue === "")
  }

  it should "remove a blocked term from a string" in {
    val originalValue = "jpeg photo"
    val enrichedValue = originalValue.applyBlockFilter(BlockList.termList)
    assert(enrichedValue === "photo")
  }

  it should "return the original string if it does not contain a blocked term" in {
    val originalValue = "photo"
    val enrichedValue = originalValue.applyBlockFilter(BlockList.termList)
    assert(enrichedValue === "photo")
  }

  "applyAllowFilter" should "return the original string if it matches the allow list" in {
    val originalValue = "moving image"
    val enrichedValue = originalValue.applyAllowFilter(AllowList.termList)
    assert(enrichedValue === "moving image")
  }

  it should "not match if the string contains an allowed term" in {
    val originalValue = "film 8mm"
    val enrichedValue = originalValue.applyAllowFilter(AllowList.termList)
    assert(enrichedValue === "film 8mm")
  }

  it should "return an empty string if the original string is not on the allow list" in {
    val originalValue = "dvd"
    val enrichedValue = originalValue.applyAllowFilter(AllowList.termList)
    assert(enrichedValue === "")
  }

  it should "match and remove extraneous white space ('  moving image  ' returns 'moving image')" in {
    val originalValue = " moving image      "
    val enrichedValue = originalValue.applyAllowFilter(AllowList.termList)
    assert(enrichedValue === "moving image")
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

  it should "not remove a period followed by a closing paren" in {
    val originalValue = "Synagogues -- Washington (D.C.)"
    val enrichedValue = originalValue.stripEndingPeriod
    val expectedValue = "Synagogues -- Washington (D.C.)"
    assert(enrichedValue === expectedValue)
  }
  
  "stripDblQuotes" should "remove all double quotes" in {
    val originalValue = """ "Hello John" """
    assert(originalValue.stripDblQuotes == " Hello John ")
  }
}
