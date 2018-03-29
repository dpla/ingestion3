package dpla.ingestion3.enrichments

import dpla.ingestion3.model.SkosConcept

/**
  * Map SkosConcepts
  */
trait SkosVocabMapper extends VocabMapper[SkosConcept] {
  // This can be defined in field mapper classes (e.g. get URI and label from file). See Robust* below
  override def vocabClass(str: String): SkosConcept = SkosConcept(concept = Some(str))
  // Default SkosConcept matching on providedLabel value. Override for
  // more control over matching specific SkosConcept fields
  override def mapVocab(value: SkosConcept, vocab: Map[String, SkosConcept]): SkosConcept =
    value.providedLabel match {
      case Some(provLabel) => vocab.getOrElse(provLabel, value) // get SkosConcept from vocab
        .copy(providedLabel = value.providedLabel) // apply provLabel to vocab SkosConcept
      case _ => value
    }
}