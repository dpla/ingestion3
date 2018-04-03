package dpla.ingestion3.enrichments

import dpla.ingestion3.model.SkosConcept
import dpla.ingestion3.utils.FileLoader

/**
  * Language mapping examples
  */
class LanguageMapper extends FileLoader with VocabMapper[SkosConcept] with VocabValidator[SkosConcept] {

  private val fileList = Seq(
    "/languages/iso639-2.csv",
    "/languages/iso639-3.csv",
    "/languages/dpla-lang.csv"
  )


  private val mapperLookup = new MapperLookup[SkosConcept](
    (term: SkosConcept) => normalizationFunc(term),
    (prov: SkosConcept, enriched: SkosConcept) => mergeFunc(prov, enriched)
  )

  private val mapperValidate = new MapperLookup[SkosConcept](
    (term: SkosConcept) => normalizationFunc(term),
    (prov: SkosConcept, enriched: SkosConcept) => mergeFunc(prov, enriched)
  )

  private def normalizationFunc(term: SkosConcept): String = term
      .providedLabel.getOrElse("")
      .toLowerCase()
      .trim

  private def mergeFunc(prov: SkosConcept, enriched: SkosConcept) =
    enriched.copy(providedLabel = prov.providedLabel)

  //noinspection TypeAnnotation
  private def loadVocab =
    getVocabFromCsvFiles(files).map(term => addLangConcept(term(0), term(1)))

  // Load vocab
  loadVocab

  //noinspection TypeAnnotation
  private def addLangConcept(provLabel: String, concept: String) = {
    // Use term abbrev for lookup key
    mapperLookup.add(
      SkosConcept(
        providedLabel = Some(provLabel),
        concept = Some(concept)
    ))
    // Use full term for lookup key
    mapperValidate.add(
      SkosConcept(
        providedLabel = Some(concept),
        concept = Some(concept)
    ))
  }

  // FileLoader
  override def files: Seq[String] = fileList

  override def enrich(value: SkosConcept): Option[SkosConcept] =
    mapperLookup.lookup(value)

  override def validate(value: SkosConcept): Option[SkosConcept] = {
    mapperValidate.lookup(value)
  }

  // Does both abv > term enrichment and validation of original value as matching controlled vocab
  def enrichLanguage(value: SkosConcept): SkosConcept = {
    (enrich(value), validate(value)) match {
      case (Some(e), _) => mapperLookup.merge(value, e)
      case (None, Some(v)) => mapperLookup.merge(value, v)
      case (_, _) => value
    }
  }


}

