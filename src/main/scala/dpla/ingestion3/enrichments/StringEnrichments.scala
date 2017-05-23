package dpla.ingestion3.enrichments

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

  def limitCharacters(value: String): String = ???

  def splitAtDelimiter(value: String): String = ???

  def splitOnProvidedLabel(value: String): String = ???

  def splitProvidedLabelAtDelimiter(value: String): String = ???

  def stripEndingPunctuation(value: String): String = ???

  def stripHTML(value: String): String = ???

  def stripLeadingPunctuation(value: String): String = ???

  def stripPunctuation(value: String): String = ???
}
