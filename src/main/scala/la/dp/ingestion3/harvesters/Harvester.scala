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
    * @param id String to hash
    * @return String md5 hash of the id value
    */
  def generateMd5(id: String): String = {
    DigestUtils.md5Hex(id)
  }
}

case class HarvesterException(message: String) extends Exception(message)