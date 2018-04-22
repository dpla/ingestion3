package dpla.ingestion3.harvesters.file

import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.harvesters.Harvester
import org.apache.avro.Schema
import org.apache.log4j.Logger

import scala.util.Try

/**
  * File based harvester
  *
  * @param shortName Provider short name
  * @param conf Configurations
  * @param outputDir Output path
  * @param logger Logger
  */
abstract class FileHarvester(shortName: String,
                             conf: i3Conf,
                             outputDir: String,
                             logger: Logger)
  extends Harvester(shortName, conf, outputDir, logger) {


  /**
    * Case class to hold the results of a file
    *
    * @param entryName Path of the entry in the file
    * @param data      Holds the data for the entry, or None if it's a directory.
    */
  case class FileResult(entryName: String, data: Option[Array[Byte]])

  /**
    * Case class hold the parsed value from a given FileResult
    */
  case class ParsedResult(id: String, item: String)

  override protected val filename: String =
    s"${shortName}_${System.currentTimeMillis()}.avro"

  /**
    * Parses and extracts ZipInputStream and writes
    * parses records out.
    *
    * @param fileResult  Case class representing extracted items from a compressed file
    * @return Count of metadata items found.
    */
  def handleFile(fileResult: FileResult,
                 unixEpoch: Long): Try[Int]

  /**
    *
    * @param inputStream
    * @return
    */
  // TODO Generalize inputStream param
  // protected def iter(inputStream: Any): Stream[FileResult]
}
