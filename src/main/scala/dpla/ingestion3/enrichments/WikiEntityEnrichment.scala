package dpla.ingestion3.enrichments

import dpla.ingestion3.model.{EdmAgent, URI}
import dpla.ingestion3.utils.FileLoader

/**
  * Language mapping examples
  */
class WikiEntityEnrichment extends FileLoader with VocabEnrichment[EdmAgent] {

  // Files to source vocabulary from
  private val fileList = Seq(
    // "/wiki/hubs.csv",
    "/wiki/institutions.csv"
  )

  // performs term lookup
  private val lookup = new VocabLookup[EdmAgent](
    (term: EdmAgent) => normalizationFunc(term)
  )

  // combine two SkosConcepts
  private val merger = new VocabMerge[EdmAgent](
    (original: EdmAgent, enriched: EdmAgent) => mergeFunc(original, enriched)
  )

  /**
    * Normalize providedLabel value for retrieval
    *
    * @param term SkosConcept
    * @return String
    */
  private def normalizationFunc(term: EdmAgent): String = term
      .name.getOrElse("")
      .toLowerCase()
      .trim

  /**
    * Merge provided and enriched values to preserve providedLabel
    *
    * @param original Original value
    * @param enriched Enriched form of `prov`
    * @return Enriched SkosConcept with original value's providedLabel
    */
  private def mergeFunc(original: EdmAgent, enriched: EdmAgent) =
    enriched.copy(name = original.name)

  /**
    * Read CSV files and load vocabulary into mappers
    *
    * @return
    */
  //noinspection TypeAnnotation,UnitMethodIsParameterless
  private def loadVocab =
    getVocabFromCsvFiles(files).foreach(term => addEntity(term(0), term(1)))

  // Load the vocab
  loadVocab

  /**
    *
    * @param entityName
    * @param entityWikiId
    */
  //noinspection TypeAnnotation
  private def addEntity(entityName: String, entityWikiId: String): Unit = {
    // Use full entity name for lookup key
    lookup.add(
      EdmAgent(
        name = Some(entityName),
        exactMatch = Seq(URI(entityWikiId))
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
  override def enrich(originalValue: EdmAgent): Option[EdmAgent] =
    lookup.lookup(originalValue)

  /**
    * Performs both full-term validation and abbreviation mapping
    * @param value Original value to be enriched
    * @return SkosConcept Enriched version of original value or original value if
    *         enrichment was not possible
    */
  def enrichEntity(value: EdmAgent): EdmAgent = {
    enrich(value) match {
      case Some(e) => merger.merge(value, e)
      case _ => value
    }
  }
}

