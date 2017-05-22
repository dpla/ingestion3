package dpla.ingestion3.enrichments

/**
  * String enrichments
  */
class StringEnrichments {
  def convertToSentenceCase(value: String): String = ???

  def limitCharacters(value: String): String = ???

  def splitAtDelimiter(value: String): String = ???

  def splitOnProvidedLabel(value: String): String = ???

  def splitProvidedLabelAtDelimiter(value: String): String = ???

  def stripEndingPunctuation(value: String): String = ???

  def stripHTML(value: String): String = ???

  def stripLeadingPunctuation(value: String): String = ???

  def stripPunctuation(value: String): String = ???
}
