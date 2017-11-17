package dpla.ingestion3.harvesters.oai.refactor


import scala.collection.TraversableOnce

/**
  * Trait that specifies OAI calls for the common functionality the data source needs.
  */

trait OaiMethods {

  def listAllRecordPagesForSet(setEither: Either[OaiError, OaiSet]):
    TraversableOnce[Either[OaiError, OaiPage]]

  def parsePageIntoRecords(pageEither: Either[OaiError, OaiPage]):
    TraversableOnce[Either[OaiError, OaiRecord]]

  def listAllSetPages(): TraversableOnce[Either[OaiError, OaiPage]]

  def parsePageIntoSets(pageEither: Either[OaiError, OaiPage]):
    TraversableOnce[Either[OaiError, OaiSet]]

  def listAllRecordPages(): TraversableOnce[Either[OaiError, OaiPage]]
}
