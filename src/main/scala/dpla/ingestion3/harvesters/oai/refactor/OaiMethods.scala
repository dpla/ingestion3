package dpla.ingestion3.harvesters.oai.refactor

import dpla.ingestion3.harvesters.oai.{OaiError, OaiPage, OaiRecord, OaiSet}

import scala.collection.TraversableOnce

/**
  * Trait that specifies OAI calls for the common functionality the data source needs.
  */

trait OaiMethods {

  def listAllRecordPagesForSet(set: String): TraversableOnce[Either[OaiRecord, OaiError]]

  def parsePageIntoRecords(page: String): TraversableOnce[Either[OaiRecord, OaiError]]

  def listAllSets: TraversableOnce[Either[OaiSet, OaiError]]

  def listAllRecordPages: TraversableOnce[Either[OaiPage, OaiError]]
}
