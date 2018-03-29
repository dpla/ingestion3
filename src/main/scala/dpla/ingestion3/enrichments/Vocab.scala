package dpla.ingestion3.enrichments

trait Vocab[T] {
  /**
    * Returns either an enriched version of T or T
    *
    * @param value Original value
    * @return T
    */
  def enrich(value: T): T

  /**
    * Accepts a String and returns T where T is a class of the
    * vocabulary being mapped to (e.g. SkosConcept, EdmAgent or
    * String).
    *
    * @param str Original string value
    * @return T
    */
  def vocabClass(str: String): T
}


