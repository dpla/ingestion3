package dpla.ingestion3.enrichments

import dpla.ingestion3.model.{EdmAgent, URI}
import dpla.ingestion3.utils.FileLoader

/**
  * Wikimedia entity enrichments
  */
class WikiEntityEnrichment extends FileLoader with VocabEnrichment[EdmAgent] {

  // Files to source vocabulary from
  private val fileList = Seq(
    "/wiki/hubs.json",
    "/wiki/institutions.json"
  )

  // performs term lookup
  private val lookup = new VocabLookup[EdmAgent](
    (term: EdmAgent) => normalizationFunc(term)
  )

  // combine two EdmAgents
  private val merger = new VocabMerge[EdmAgent](
    (original: EdmAgent, enriched: EdmAgent) => mergeFunc(original, enriched)
  )

  /**
    * Normalize providedLabel value for retrieval
    *
    * @param term EdmAgent
    * @return String
    */
  private def normalizationFunc(term: EdmAgent): String =
    term.name.getOrElse("").toLowerCase.trim

  /**
    * Merge provided and enriched values to preserve original 'name' value
    *
    * @param original Original value
    * @param enriched Enriched form of 'original' value
    * @return Enriched EdmAgent with original value's 'name'
    */
  private def mergeFunc(original: EdmAgent, enriched: EdmAgent) =
    enriched.copy(name = original.name)

  /**
    * Read JSON files and load vocabulary
    *
    * @return
    */
  //noinspection TypeAnnotation,UnitMethodIsParameterless
  private def loadVocab =
    getVocabFromJsonFiles(files).foreach { case (key: String, value: String) => addEntity(key, value) }

  // Load the vocab
  loadVocab

  /**
    * Add EdmAgent to lookup dataset
    *
    * @param entityName Key from JSON file
    * @param entityWikiId Value from JSON file
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
    * Get enriched form of the given entity by mapping
    * entity name to entity name (case-insensitive)
    * Example:
    *   'university of pennsylvania' -> 'University of Pennsylvania'
    *
    * @param originalValue Original value
    * @return T Enriched value
    */
  override def enrich(originalValue: EdmAgent): Option[EdmAgent] =
    lookup.lookup(originalValue)

  /**
    * Performs full-term validation and mapping
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

