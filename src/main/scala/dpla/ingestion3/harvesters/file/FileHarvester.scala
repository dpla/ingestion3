package dpla.ingestion3.harvesters.file

import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.harvesters.{Harvester, LocalHarvester}
import org.apache.avro.generic.GenericData
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import scala.util.Try

/**
  *
  * File based harvester
  *
  * @param shortName Provider short name
  * @param conf Configurations
  * @param outputDir Output path
  * @param logger Logger
  */
abstract class FileHarvester(spark: SparkSession,
                             shortName: String,
                             conf: i3Conf,
                             outputDir: String,
                             logger: Logger)
  extends LocalHarvester(spark, shortName, conf, outputDir, logger) {


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
    * Writes item out
    *
    * @param unixEpoch Timestamp of the harvest
    * @param item Harvested record
    *
    */
  //todo this is in harvester?
  def writeOut(unixEpoch: Long, item: ParsedResult): Unit = {
    val avroWriter = getAvroWriter
    val genericRecord = new GenericData.Record(Harvester.schema)
    genericRecord.put("id", item.id)
    genericRecord.put("ingestDate", unixEpoch)
    genericRecord.put("provider", shortName)
    genericRecord.put("document", item.item)
    genericRecord.put("mimetype", mimeType)
    avroWriter.append(genericRecord)
  }

}
