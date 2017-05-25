package dpla.ingestion3.enrichments

import org.apache.commons.lang.StringEscapeUtils
import org.jsoup.Jsoup
import org.jsoup.nodes.Document.OutputSettings
import org.jsoup.nodes.Entities.EscapeMode
import org.jsoup.safety.Cleaner
import org.jsoup.safety.Whitelist

import scala.util.matching.Regex

/**
  * String enrichments
  *
  */
class StringEnrichments {


  /**
    * Accepts a String value and splits it around periods. Strips
    * trailing and leading whitespace from those "sentences" and
    * capitalizes the first character of each sentence leaving all
    * other characters alone.
    *
    * We do not assume that all upper case characters should be
    * downcased.
    *
    * @param value Original harvested value from provider
    * @return
    */
  def convertToSentenceCase(value: String): String = {
    val pattern: Regex = """.*?(\.)""".r
    val sentences = for( t <- pattern findAllIn value) yield t.trim.capitalize
    // rejoin the sentences and add back the whitespace that was trimmed off
    sentences.mkString(" ")
  }

  def limitCharacters(value: String): String = {
    ""
  }

  /**
    * Splits a String value around a given delimiter. The default delimiter
    * is a semi-colon.
    *
    * @param value      The string value to split
    * @param delimiter  The delimiter to split on
    * @return
    */
  def splitAtDelimiter(value: String, delimiter: String = ";"): Array[String] = {
    value.split(delimiter).map(_.trim)
  }

  def splitOnProvidedLabel(value: String): Array[String]= {
    Array("")
  }

  def splitProvidedLabelAtDelimiter(value: String): Array[String] = {
    Array("")
  }

  def stripEndingPunctuation(value: String): String = {
    ""
  }

  def stripHTML(value: String): String = {
    val unescaped = StringEscapeUtils.unescapeHtml(value)
    val cleaned = Jsoup.clean(unescaped, "", Whitelist.none(), new OutputSettings().escapeMode(EscapeMode.xhtml))
    StringEscapeUtils.unescapeHtml(cleaned)
  }

  def stripLeadingPunctuation(value: String): String = {
    ""
  }

  def stripPunctuation(value: String): String = {
    ""
  }
}
