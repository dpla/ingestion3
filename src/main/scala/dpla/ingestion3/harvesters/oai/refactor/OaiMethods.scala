package dpla.ingestion3.harvesters.oai.refactor


import scala.collection.TraversableOnce

/**
 * Trait that specifies OAI calls for the common functionality the data source needs.
 */

trait OaiMethods {

  def listAllRecordPagesForSet(setEither: Either[OaiError, OaiSet]): IterableOnce[Either[OaiError, OaiPage]]

  def parsePageIntoRecords(pageEither: Either[OaiError, OaiPage], removeDeleted: Boolean): IterableOnce[Either[OaiError, OaiRecord]]

  def listAllSetPages(): IterableOnce[Either[OaiError, OaiPage]]

  def parsePageIntoSets(pageEither: Either[OaiError, OaiPage]): IterableOnce[Either[OaiError, OaiSet]]

  def listAllRecordPages(): IterableOnce[Either[OaiError, OaiPage]]
}
