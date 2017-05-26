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
    */
  val convertToSentenceCase: SingleStringEnrichment = (value) => {
    val pattern: Regex = """.*?(\.)""".r
    val sentences = for( t <- pattern findAllIn value) yield t.trim.capitalize
    // rejoin the sentences and add back the whitespace that was trimmed off
    sentences.mkString(" ")
  }

  val limitCharacters: SingleStringEnrichment = (value) => {
    ""
  }

  /**
    * Splits a String value around a given delimiter.
    */
  val splitAtDelimiter: (String, String) => Array[String] = (value, delimiter) => {
    value.split(delimiter).map(_.trim)
  }

  /**
    * Splits a String around semicolon
    */
  val splitAtSemicolons: MultiStringEnrichment = splitAtDelimiter(_, ";")

  val stripEndingPunctuation: SingleStringEnrichment = (value) => {
    value.replace("""[^\w\'\"\s]+$""", "")
  }

  val stripHTML: SingleStringEnrichment = (value) => {
    val unescaped = StringEscapeUtils.unescapeHtml(value)
    val cleaned = Jsoup.clean(unescaped, "", Whitelist.none(), new OutputSettings().escapeMode(EscapeMode.xhtml))
    StringEscapeUtils.unescapeHtml(cleaned)
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
