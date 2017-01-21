package la.dp.ingestion3.harvesters

import org.apache.commons.codec.digest.DigestUtils

/**
  * Created by Scott on 1/21/17.
  */
object Harvester {
  /**
    * This one liner taken from:
    * http://stackoverflow.com/questions/38855843/scala-one-liner-to-generate-md5-hash-from-string
    * MessageDigest.getInstance("MD5").digest(id.getBytes()).map(0xFF & _).map { "%02x".format(_) }.foldLeft("") {_ + _}
    */
  def generateMd5(id: String): String = {
    DigestUtils.md5Hex(id)
  }
}
