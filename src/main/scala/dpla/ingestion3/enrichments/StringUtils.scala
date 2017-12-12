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
object StringUtils {

  implicit class Enrichments(value: String) {

    type SingleStringEnrichment = String
    type MultiStringEnrichment = Array[String]

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
    val convertToSentenceCase: SingleStringEnrichment = {
      val pattern: Regex = """.*?(\.)""".r
      val sentences = for( t <- pattern findAllIn value) yield t.trim.capitalize
      // rejoin the sentences and add back the whitespace that was trimmed off
      sentences.mkString(" ")
    }

    val limitCharacters: (Int) => (String) = (length) => {
      if (value.length > length) value.substring(0, length)
      else value
    }

    /**
      * Splits a String value around a given delimiter.
      */
    val splitAtDelimiter: (String) => Array[String] = (delimiter) => {
      value.split(delimiter).map(_.trim)
    }

    val stripEndingPunctuation: SingleStringEnrichment = {
      value.replace("""[^\w\'\"\s]+$""", "")
    }

    val stripHTML: SingleStringEnrichment = {
      val unescaped = StringEscapeUtils.unescapeHtml(value)
      val cleaned = Jsoup.clean(unescaped, "", Whitelist.none(), new OutputSettings().escapeMode(EscapeMode.xhtml))
      StringEscapeUtils.unescapeHtml(cleaned)
    }

    val stripLeadingPunctuation: SingleStringEnrichment = {
      value.replace("""^[^\w\'\"\s]+""", "")
    }

    val stripPunctuation: SingleStringEnrichment = {
      value.replaceAll("""[^\w\'\"\s]""", "")
    }

    val reduceWhitespace: SingleStringEnrichment = {
      value.replaceAll("  ", " ")
    }
  }
}
