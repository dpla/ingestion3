package dpla.ingestion3.harvesters

import org.apache.commons.codec.digest.DigestUtils

import scala.xml.Node

/**
  * Base harvester
  */
object Harvester {
  /**
    * The only shared method between harvester implementations. Generates
    * an md5 hash of the id value
    *
    * @param id String
    * @param prov String
    *             Optional, Provider abbreviation if provided is prepended to
    *             id and then hashed
    * @return String md5 hash of the id value
    */
  def generateMd5(id: String, prov: String=""): String = {
    if(prov.nonEmpty)
      DigestUtils.md5Hex(List(prov,id).mkString("--").trim)
    DigestUtils.md5Hex(id)
  }

  /**
    * Accepts a record from an OAI feed an returns the local identifier
    * TODO: parameterize the path to the ID
    *
    * @param record String
    *               The original record from the OAI feed
    * @return The local identifier
    */
  def getLocalId(record: Node): String = {
    (record \\ "header" \\ "identifier").text
  }
}

/**
  * Basic exception class, minimal implementation
  *
  * @param message String
  */
case class HarvesterException(message: String) extends Exception(message)