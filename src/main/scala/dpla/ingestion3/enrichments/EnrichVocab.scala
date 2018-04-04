package dpla.ingestion3.enrichments

/**
  * Generic vocabulary builder and and term lookup
  *
  * @tparam T Class of vocab (e.g. SkosConcept, String, EdmAgent)
  */

class VocabLookup[T](normalizationFunc: (T) => String) {

  // Vocab mapping
  private val data = scala.collection.mutable.Map[String, T]()

  /**
    * Add vocabulary map to `data`
    *
    * @param vocabulary Map[String,T] Terms to add
    */
  def add(vocabulary: Map[String, T]) = data ++= vocabulary.toList

  /**
    * Adds term to vocab mapping and normalizes key value for retrieval
    *
    * @param term T Term to add
    */
  def add(term: T) = data += normalizationFunc(term) -> term

  /**
    * Get term from vocab mapping
    *
    * @param originalRecord Original term
    * @return Enriched form of `originalRecord`
    */
  def lookup(originalRecord: T): Option[T] =
    data.get(normalizationFunc(originalRecord))

  /**
    * Print the vocab mapping
    */
  def print = data.keys.foreach(key => println(s"$key->${data.getOrElse(key, "*NO VALUE FOR KEY*")}"))
}

/**
  * Meerge vocabulary terms
  *
  * @param mergeFunc Defines how to perform the merge
  */
class VocabMerge[T](mergeFunc: (T,T) => T){
  /**
    * Merge original and enriched values into a single term
    * @param originalRecord
    * @param enrichedRecord
    * @return
    */
  def merge(originalRecord: T, enrichedRecord: T) = mergeFunc(originalRecord, enrichedRecord)
}

/**
  * Vocabulary enrichment
  *
  * @tparam T Class of vocabulary to enrich
  */
trait VocabEnrichment[T] {
  /**
    * Returns either an enriched version of T or None
    *
    * @param value Original value
    * @return T
    */
  def enrich(value: T): Option[T]
}