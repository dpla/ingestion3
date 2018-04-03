package dpla.ingestion3.enrichments

import dpla.ingestion3.model.SkosConcept
import dpla.ingestion3.utils.FileLoader

/**
  * Language mapping examples
  */
class LanguageMapper extends FileLoader with VocabMapper[SkosConcept] with VocabValidator[SkosConcept] {

  // Files to source vocabulary from
  private val fileList = Seq(
    "/languages/iso639-2.csv",
    "/languages/iso639-3.csv",
    "/languages/dpla-lang.csv"
  )

  private val lookup = new MapperLookup[SkosConcept](
    (term: SkosConcept) => normalizationFunc(term)
  )

  private val validator = new MapperLookup[SkosConcept](
    (term: SkosConcept) => normalizationFunc(term)
  )

  private val merger = new MapperMerge[SkosConcept](
    (prov: SkosConcept, enriched: SkosConcept) => mergeFunc(prov, enriched)
  )

  /**
    * Normalize providedLabel value for retrieval
    *
    * @param term SkosConcept
    * @return String
    */
  private def normalizationFunc(term: SkosConcept): String = term
      .providedLabel.getOrElse("")
      .toLowerCase()
      .trim

  /**
    * Merge provided and enriched values to preserve providedLabel
    *
    * @param prov Original value
    * @param enriched Enriched form of `prov`
    * @return Enriched SkosConcept with original value's providedLabel
    */
  private def mergeFunc(prov: SkosConcept, enriched: SkosConcept) =
    enriched.copy(providedLabel = prov.providedLabel)

  /**
    * Read CSV files and load vocabulary into mappers
    *
    * @return
    */
  //noinspection TypeAnnotation
  private def loadVocab(): Unit =
    getVocabFromCsvFiles(files).foreach(term => addLangConcept(term(0), term(1)))

  // Load the vocab(s)
  loadVocab()

  /**
    * Create SkosConcepts from provLabel and concept and add to
    * `lookup` and `validator`
    *
    * @param langAbbv Language abbreviation
    * @param langTerm Full language term
    * @return
    */
  //noinspection TypeAnnotation
  private def addLangConcept(langAbbv: String, langTerm: String): Unit = {
    // Use term abbrev for lookup key
    lookup.add(
      SkosConcept(
        providedLabel = Some(langAbbv),
        concept = Some(langTerm)
    ))
    // Use full term for lookup key
    validator.add(
      SkosConcept(
        providedLabel = Some(langTerm),
        concept = Some(langTerm)
    ))
  }

  // FileLoader
  override def files: Seq[String] = fileList

  /**
    * Get enriched form of the given language by mapping
    * language abbreviation to a full term
    * Example:
    *   'Eng' -> 'English'
    *
    * @param originalValue Original value
    * @return T Enriched value
    */
  override def enrich(originalValue: SkosConcept): Option[SkosConcept] =
    lookup.lookup(originalValue)

  /**
    * Get enriched form of the given language by comparing
    * full terms.
    * Example:
    *   'english' -> 'English'
    *
    * @param originalValue Original value
    * @return T Enriched value
    */
  override def validate(originalValue: SkosConcept): Option[SkosConcept] = {
    validator.lookup(originalValue)
  }

  // Does both abv > term enrichment and validation of original value as matching controlled vocab
  /**
    * Performs both full-term validation and abbreviation mapping
    * @param value Original value to be enriched
    * @return SkosConcept Enriched version of original value or original value if
    *         enrichment was not possible
    */
  def enrichLanguage(value: SkosConcept): SkosConcept = {
    (enrich(value), validate(value)) match {
      case (Some(e), _) => merger.merge(value, e)
      case (None, Some(v)) => merger.merge(value, v)
      case (_, _) => value
    }
  }


}

