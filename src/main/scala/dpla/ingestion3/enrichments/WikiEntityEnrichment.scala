package dpla.ingestion3.enrichments

import dpla.ingestion3.mappers.utils.JsonExtractor
import dpla.ingestion3.model.{EdmAgent, URI, nameOnlyAgent}
import dpla.ingestion3.utils.FileLoader
import dpla.ingestion3.wiki.WikiUri
import org.json4s.jackson.JsonMethods.parse

import scala.io.Source

/**
  * Wikimedia entity enrichments
  */
class WikiEntityEnrichment extends FileLoader with VocabEnrichment[EdmAgent] with JsonExtractor with WikiUri {

  // Files to source vocabulary from
  private val fileList = Seq(
      "/wiki/institutions_v2.json"
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
    original.copy(exactMatch = enriched.exactMatch)
    // enriched.copy(name = original.name)

  /**
    * Read JSON files and load vocabulary
    *
    * @return
    */
  //noinspection TypeAnnotation,UnitMethodIsParameterless
  private def loadVocab =
    getInstitutionVocab(files).foreach { case (key: String, value: String) => addEntity(key, value) }

  private def getInstitutionVocab(files: Seq[String]): Seq[(String, String)] = {
    files.flatMap(file => {
      val fileContentString = Source.fromInputStream(getClass.getResourceAsStream(file)).getLines().mkString

      val json = parse(fileContentString)

      val dataProviderKeys = extractKeys(json)
        .map(dataProvider => s"$dataProvider" -> extractString(json \ dataProvider \ "Wikidata").get)

      val institutionKeys = extractKeys(json) // get dataProvider keys
        .flatMap(dataProvider => {
          extractKeys(json \ dataProvider \ "institutions") // get institutions keys
            .map(institution => {
            s"$dataProvider$institution" -> extractString(json \ dataProvider \ "institutions" \ institution \ "Wikidata").get
          })
        })

      dataProviderKeys ++ institutionKeys
    })
  }
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
        exactMatch = Seq(URI(s"${baseWikiUri}$entityWikiId"))
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
    * @param qualifierValue
    *
    * @return EdmAgent Enriched version of original value or original value if
    *         enrichment was not possible
    */
  def enrichEntity(value: EdmAgent, qualifierValue: Option[EdmAgent] = None): EdmAgent = {
    val entity = qualifierValue match {
      case Some(qv) => nameOnlyAgent(s"${qv.name.getOrElse("")}${value.name.getOrElse("")}")
      case _ => value
    }

    enrich(entity) match {
      case Some(e) => merger.merge(value, e)
      case _ => value
    }
  }
}

