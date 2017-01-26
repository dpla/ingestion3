package la.dp.ingestion3.harvesters

import java.io.File
import java.nio.file.Files
import java.nio.file.StandardOpenOption.CREATE
import java.nio.file.StandardOpenOption.TRUNCATE_EXISTING

import org.apache.commons.codec.digest.DigestUtils

import scala.xml.Node


/**
  * Created by Scott on 1/21/17.
  */
object Harvester {
  /**
    * @param id Value to hash, expected to be the record's local identifier
    * @return md5 hash of id
    */
  def generateMd5(id: String): String = {
    DigestUtils.md5Hex(id)
  }
}
