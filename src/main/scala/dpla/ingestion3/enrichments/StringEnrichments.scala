package dpla.ingestion3.enrichments

import org.apache.commons.lang.StringEscapeUtils
import org.jsoup.Jsoup
import org.jsoup.nodes.Document.OutputSettings
import org.jsoup.nodes.Entities.EscapeMode
import org.jsoup.safety.Whitelist

import scala.util.matching.Regex

/**
  * String enrichments
  *
  */
class StringEnrichments {

  type SingleStringEnrichment = (String) => String
  type MultiStringEnrichment = (String) => Array[String]

  /**
    * Tokenize the provided string around period (.) and
    * capitalizes the first character of each token.
    *
    * Takes a string value and splits it around periods. Strips
    * trailing and leading whitespace from those "sentences" and
    * capitalizes the first character of each sentence leaving all
    * other characters alone.
    *
    * @example
    *          "hello Ted.   nice to see you today." =>
    *           "Hello Ted. Nice to see you today."
    */
  val convertToSentenceCase: SingleStringEnrichment = (value) => {
    val pattern: Regex = """.*?(\.)""".r
    val sentences = for( t <- pattern findAllIn value) yield t.trim.capitalize
    // rejoin the sentences and add back the whitespace that was trimmed off
    sentences.mkString(" ")
  }

  // preSpacePat is precompiled once for limitCharacters, below. A string with
  // spaces, capturing the part up to the last space.
  val preSpacePat = """^(.+)\s\S+$""".r

  /**
    * Limit a string to the given number of characters.
    *
    * Truncates the string on whitespace and adds ellipses on the end, where
    * it can.
    *
    * @example
    *          ("Lorem ipsum", 10) => "Lorem..."
    */
  val limitCharacters: (String, Int) => String = (value, limit) => {
    value match {
      case s if s.length > limit && limit > 3 => {
        val sliced = s.slice(0, limit - 3) // -3 is OK if string is < 3 chars
        sliced match {
          case preSpacePat(x) => s"$x..."  // add ellipses
          case _ => sliced                 // not space-delimited?
        }
      }
      case s if limit <= 3 => value.slice(0, limit)
      case _ => value  // Length is OK as-is.
    }
  }

  /**
    * Splits a String value around a given delimiter.
    *
    * @example
    *          ("ernie, bert, maria", ",") => ("ernie", "bert", "maria")
    */
  val splitAtDelimiter: (String, String) => Array[String] = (value, delimiter) => {
    value.split(delimiter).map(_.trim)
  }

  /**
    * Splits a String around semicolon
    *
    * @example
    *          ("one; two, three", ";") => ("one", "two, three")
    */
  val splitAtSemicolons: MultiStringEnrichment = splitAtDelimiter(_, ";")

  /**
    * Removes punctuation from the end of a given String
    *
    * @example
    *          "Hello Frank!##$*." => "Hello Frank"
    */
  val stripEndingPunctuation: SingleStringEnrichment = (value) => {
    value.replace("""[^\w\'\"\s]+$""", "")
  }

  /**
    * Strips HTML from a string. Does a best effort around not emitting HTML entities or turning HTML entities into
    * actual HTML
    *
    * @example
    *          "It's <marquee>PEANUT BUTTER JELLY TIME</marquee>" => "It's PEANUT BUTTER JELLY TIME"
    */
  val stripHTML: SingleStringEnrichment = (value) => {
    val unescaped = StringEscapeUtils.unescapeHtml(value)
    val cleaned = Jsoup.clean(unescaped, "", Whitelist.none(), new OutputSettings().escapeMode(EscapeMode.xhtml))
    StringEscapeUtils.unescapeHtml(cleaned)
  }

  /**
    * Removes leading colons and semicolons from a given String.
    *
    * @note
    *       Only run against the Title field.
    *
    * @example
    *          ":;; Happy birthday! ;;:" => "Happy birthday! ;;:"
    */
  val stripLeadingColons: SingleStringEnrichment = (value) => {
    val pattern: Regex = """^[\;\:]*""".r
    pattern.replaceAllIn(value, "")
  }

  val stripLeadingPunctuation: SingleStringEnrichment = (value) => {
    value.replace("""^[^\w\'\"\s]+""", "")
  }

  val stripPunctuation: SingleStringEnrichment = (value) => {
    value.replaceAll("""[^\w\'\"\s]""", "")
  }

  /**
    * Removes all leading, trailing whitespace and replaces
    * duplicate whitespace with a single value for a given
    * String value
    *
    * @example  " one    two  three  " => "one two three"
    */
  val stripWhitespace: SingleStringEnrichment = (value) => {
    value.trim.replaceAll(" +", " ");
  }
}
