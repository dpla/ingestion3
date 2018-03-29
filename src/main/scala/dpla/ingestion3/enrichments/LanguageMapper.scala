package dpla.ingestion3.enrichments

import dpla.ingestion3.model.SkosConcept
import dpla.ingestion3.utils.FileLoader

/**
  * Language mapping examples
  */
object LanguageMapper extends SkosVocabMapper with FileLoader with VocabMapLoader[SkosConcept] {
  // FileLoader
  override def files: Seq[String] = Seq(
    "/languages/iso639-2.csv",
    "/languages/iso639-3.csv",
    "/languages/dpla-lang.csv"
  )

  // VocabMapper
  override lazy val vocab: Map[String, SkosConcept] = loadVocab

  override def loadVocab: Map[String, SkosConcept] = getVocabFromCsvFiles(files)
    .map(term => term(0) -> vocabClass(term(1)))
    .toMap
}

