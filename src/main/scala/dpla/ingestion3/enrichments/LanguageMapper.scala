package dpla.ingestion3.enrichments

import dpla.ingestion3.model.SkosConcept
import dpla.ingestion3.utils.FileLoader

/**
  * Language mapping examples
  */
class LanguageMapper extends FileLoader with VocabMapper[SkosConcept] {

  private val mapperLookup = new MapperLookup[SkosConcept] (
    // Normalizations terms for retrieval
    (term: SkosConcept) => term
      .providedLabel.getOrElse("")
      .toLowerCase()
      .trim,
    // Merges enriched and original values
    (prov: SkosConcept, enriched: SkosConcept) => enriched.copy(providedLabel = prov.providedLabel),
    // Validates provided term against controlled vocab terms
    (prov: SkosConcept, enriched: SkosConcept) => prov
      .providedLabel.getOrElse("")
      .trim
      .equalsIgnoreCase(enriched.concept.get.trim)
  )

  // Load vocab
  loadVocab

  // FileLoader
  override def files: Seq[String] = Seq(
    "/languages/iso639-2.csv",
    "/languages/iso639-3.csv",
    "/languages/dpla-lang.csv"
  )

  //noinspection TypeAnnotation
  private def loadVocab = {
    getVocabFromCsvFiles(files)
      .map(term => {
        mapperLookup.add(
          SkosConcept(
            providedLabel = Some(term(0)),
            concept = Some(term(1)))
        )
      })
  }

  // Does both abv > term enrichment and validation of original value as matching controlled vocab
  def enrichLanguage(value: SkosConcept): SkosConcept = {
    (enrich(value), validate(value)) match {
      case (Some(e), _) => mapperLookup.merge(value, e)
      case (None, Some(v)) => mapperLookup.merge(value, v)
      case (None, None) => value
    }
  }
  override def enrich(value: SkosConcept): Option[SkosConcept] = {
    mapperLookup
      .lookup(value)
  }

  override def validate(value: SkosConcept): Option[SkosConcept] = {
    mapperLookup
      .validate(value)
  }
}

