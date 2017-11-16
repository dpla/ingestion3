package dpla.ingestion3.harvesters.oai.refactor


import scala.collection.TraversableOnce

/**
  * Trait that specifies OAI calls for the common functionality the data source needs.
  */

trait OaiMethods {

  def listAllRecordPagesForSet(setEither: Either[OaiSet, OaiError]): TraversableOnce[Either[OaiRecord, OaiError]]

  def parsePageIntoRecords(pageEither: Either[OaiPage, OaiError]): TraversableOnce[Either[OaiRecord, OaiError]]

  def listAllSets(): TraversableOnce[Either[OaiSet, OaiError]]

  def listAllRecordPages():
    TraversableOnce[Either[OaiPage, OaiError]]
}
