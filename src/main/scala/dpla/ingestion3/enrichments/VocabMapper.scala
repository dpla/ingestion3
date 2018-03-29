package dpla.ingestion3.enrichments

/**
  * Generic vocab mapper
  *
  * @tparam T Class of vocab (e.g. SkosConcept, String, EdmAgent)
  */
trait VocabMapper[T] extends Vocab[T] {
  // Mapping of uncontrolled/non-preferred values to controlled terms
  // TODO Create subclass of Map[String, T] with overloaded equals or elemEquals to support case agnostic matching
  val vocab: Map[String, T]

  /**
    * Returns enriched version of value or value
    * @param value Original value
    * @return T
    */
  override def enrich(value: T): T =
    mapVocab(value, vocab) // .getOrElse(value)

  /**
    *
    * @param value Original term
    * @param vocab Controlled vocabulary to lookup term against
    * @return
    */
  def mapVocab(value: T, vocab: Map[String, T]): T =
    vocab.get(value.toString).asInstanceOf[T]
}

/**
  * Loads vocabulary terms
 */
trait VocabMapLoader[T] extends Vocab[T] {
  /**
    * Load vocabulary terms from any source
    *
    * @return Map[String, T]
    */
  def loadVocab: Map[String, T]
}
