package dpla.ingestion3.enrichments

trait VocabEnforcer[T] {
  val enforceVocab: (T, Set[T]) => Boolean = (value, vocab) => {
    vocab.contains(value)
  }
}

object DcmiTypeEnforcer extends VocabEnforcer[String] {

  val DcmiTypes = Set(
    "Collection",
    "Dataset",
    "Event",
    "Image",
    "Interactive Resource",
    "Service",
    "Software",
    "Sound",
    "Text"
  )

  val enforceDcmiType: (String) => Boolean = enforceVocab(_, DcmiTypes)
}
