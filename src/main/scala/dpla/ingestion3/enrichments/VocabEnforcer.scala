package dpla.ingestion3.enrichments

import dpla.ingestion3.mappers.rdf.DCMIType


trait VocabEnforcer[T] {
  val enforceVocab: (T, Set[T]) => Boolean = (value, vocab) => {
    vocab.contains(value)
  }
}

object DcmiTypeEnforcer extends VocabEnforcer[String] {
  val dcmiType = DCMIType()

  val DcmiTypeStrings = Set(
    dcmiType.Collection,
    dcmiType.Dataset,
    dcmiType.Event,
    dcmiType.Image,
    dcmiType.InteractiveResource,
    dcmiType.MovingImage,
    dcmiType.PhysicalObject,
    dcmiType.Service,
    dcmiType.Software,
    dcmiType.Sound,
    dcmiType.StillImage,
    dcmiType.Text
  ).map(_.getLocalName)

  val enforceDcmiType: (String) => Boolean = enforceVocab(_, DcmiTypeStrings)
}
