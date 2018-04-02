package dpla.ingestion3.enrichments

/**
  * Generic vocab mapper lookup
  *
  * @tparam T Class of vocab (e.g. SkosConcept, String, EdmAgent)
  */

class MapperLookup[T]
  (normalizationFunc: (T) => String,
   mergeFunc: (T,T) => T,
   validationFunc: (T,T) => Boolean) {

  private val data = scala.collection.mutable.Map[String, T]()

  //noinspection TypeAnnotation
  def add(originalRecord: T) = data += normalizationFunc(originalRecord) -> originalRecord

  def lookup(originalRecord: T): Option[T] = data.get(normalizationFunc(originalRecord))

  def merge(originalRecord: T, enrichedRecord: T) = mergeFunc(originalRecord, enrichedRecord)

  def validate(originalRecord: T): Option[T] = data.values.find(validationFunc(originalRecord, _))

  def print(): Unit = data.keys.foreach(key => println(s"$key -> ${data.get(key)}"))
}

/**
  * VocabMapper
  *
  * @tparam T
  */
trait VocabMapper[T] {
  /**
    * Returns either an enriched version of T or None
    *
    * @param value Original value
    * @return T
    */
  def enrich(value: T): Option[T]

  /**
    * Determines if the original value is already a standard term in the
    * controlled vocabulary. 'English' does not need to be enriched b/c it
    * is already in the correct form
    *
    * @param value Original value
    * @return T
    */
  def validate(value: T): Option[T]
}
