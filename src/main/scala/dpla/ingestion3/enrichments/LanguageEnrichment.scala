package dpla.ingestion3.enrichments

import dpla.ingestion3.model.SkosConcept
import dpla.ingestion3.utils.FileLoader

/** Language mapping examples
  */
class LanguageEnrichment extends FileLoader with VocabEnrichment[SkosConcept] {

  // Files to source vocabulary from
  private val fileList = Seq(
    "/languages/deduped-iso639x.csv",
    "/languages/dpla-lang.csv"
  )

  // performs term lookup
  private val lookup = new VocabLookup[SkosConcept]((term: SkosConcept) =>
    normalizationFunc(term)
  )

  // combine two SkosConcepts
  private val merger =
    new VocabMerge[SkosConcept]((prov: SkosConcept, enriched: SkosConcept) =>
      mergeFunc(prov, enriched)
    )

  /** Normalize providedLabel value for retrieval
    *
    * @param term
    *   SkosConcept
    * @return
    *   String
    */
  private def normalizationFunc(term: SkosConcept): String = term.providedLabel
    .getOrElse("")
    .toLowerCase()
    .trim

  /** Merge provided and enriched values to preserve providedLabel
    *
    * @param prov
    *   Original value
    * @param enriched
    *   Enriched form of `prov`
    * @return
    *   Enriched SkosConcept with original value's providedLabel
    */
  private def mergeFunc(prov: SkosConcept, enriched: SkosConcept): SkosConcept =
    enriched.copy(providedLabel = prov.providedLabel)

  /** Read CSV files and load vocabulary into mappers
    *
    * @return
    */
  // noinspection TypeAnnotation,UnitMethodIsParameterless
  private def loadVocab: Unit =
    getVocabFromCsvFiles(files).foreach(term =>
      addLangConcept(term(0), term(1))
    )

  // Load the vocab
  loadVocab

  /** Adds to lookup map SkosConcepts using both provLabel and concept for the
    * lookup key
    */
  private def addLangConcept(languageAbbreviation: String, langTerm: String): Unit = {
    // Use term abbrev for lookup key
    lookup.add(
      SkosConcept(
        providedLabel = Some(languageAbbreviation),
        concept = Some(langTerm)
      )
    )
    // Use full term for lookup key
    lookup.add(
      SkosConcept(
        providedLabel = Some(langTerm),
        concept = Some(langTerm)
      )
    )
  }

  // FileLoader
  override def files: Seq[String] = fileList

  /** Get enriched form of the given language by mapping language abbreviation
    * to a full term Example: 'Eng' -> 'English'
    *
    * @param originalValue
    *   Original value
    * @return
    *   T Enriched value
    */
  override def enrich(originalValue: SkosConcept): Option[SkosConcept] =
    lookup.lookup(originalValue)

  /** Performs both full-term validation and abbreviation mapping
    * @param value
    *   Original value to be enriched
    * @return
    *   SkosConcept Enriched version of original value or original value if
    *   enrichment was not possible
    */
  def enrichLanguage(value: SkosConcept): SkosConcept =
    enrich(value) match {
      case Some(e) => merger.merge(value, e)
      case _       => value
    }
}
