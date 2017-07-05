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
    * Accepts a String value and splits it around periods. Strips
    * trailing and leading whitespace from those "sentences" and
    * capitalizes the first character of each sentence leaving all
    * other characters alone.
    *
    * We do not assume that all upper case characters should be
    * downcased.
    *
    */
  val convertToSentenceCase: SingleStringEnrichment = (value) => {
    val pattern: Regex = """.*?(\.)""".r
    val sentences = for( t <- pattern findAllIn value) yield t.trim.capitalize
    // rejoin the sentences and add back the whitespace that was trimmed off
    sentences.mkString(" ")
  }

  val limitCharacters: (String, Int) => (String) = (value, length) => {
    if (value.length > length) value.substring(0, length)
    else value
  }

  /**
    * Splits a String value around a given delimiter.
    */
  val splitAtDelimiter: (String, String) => Array[String] = (value, delimiter) => {
    value.split(delimiter).map(_.trim)
  }

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
}
