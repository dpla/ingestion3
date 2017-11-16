package dpla.ingestion3.harvesters.oai.refactor

import dpla.ingestion3.harvesters.oai.{OaiError, OaiPage, OaiRecord, OaiSet}

import scala.collection.TraversableOnce

trait OaiMethods {

  def listAllRecordPagesForSet(set: String): TraversableOnce[Either[OaiRecord, OaiError]]

  def parsePageIntoRecords(page: String): TraversableOnce[Either[OaiRecord, OaiError]]

  def listAllSets: TraversableOnce[Either[OaiSet, OaiError]]

  def listAllRecordPages: TraversableOnce[Either[OaiPage, OaiError]]
}
