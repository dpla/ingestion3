package la.dp.ingestion3.harvesters

import org.apache.commons.codec.digest.DigestUtils

/**
  * Base harvester
  */
object Harvester {
  /**
    * The only shared method between harvester implementations. Generates
    * an md5 hash of the id value
    *
    * @param id String
    *           Provider's record id to hash
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
}

case class HarvesterException(message: String) extends Exception(message)