package dpla.ingestion3.harvesters.file

import java.io.{File, FileInputStream}
import java.util.zip.ZipInputStream
import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.harvesters.Harvester
import dpla.ingestion3.mappers.utils.XmlExtractor
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import dpla.ingestion3.harvesters.file.FileFilters.zipFilter
import dpla.ingestion3.model.AVRO_MIME_XML
import org.apache.avro.generic.GenericData
import org.apache.logging.log4j.LogManager

import scala.util.{Failure, Success, Try}
import scala.xml.{MinimizeMode, Node, Utility, XML}

/** Extracts values from parsed Xml
  */
class VaFileExtractor extends XmlExtractor

/** Entry for performing a Digital Virginias file harvest
  */
class VaFileHarvester(
    spark: SparkSession,
    shortName: String,
    conf: i3Conf
) extends FileHarvester(spark, shortName, conf) {

  def mimeType: GenericData.EnumSymbol = AVRO_MIME_XML

  protected val extractor = new VaFileExtractor()



  /** Main logic for handling individual entries in the zip.
    *
    * @param zipResult
    *   Case class representing extracted item from the zip
    * @return
    *   Count of metadata items found.
    */
  def handleFile(zipResult: FileResult, unixEpoch: Long): Try[Int] =
    zipResult.data match {
      case None =>
        Success(0) // a directory, no results

      case Some(data) =>
        Try {
          Try {
            XML.loadString(new String(data)) // parse string to XML
          } match {
            case Success(xml) =>
              val id: String = zipResult.entryName
              val outputXml: String = Harvester.xmlToString(xml)
              val item: ParsedResult = ParsedResult(id, outputXml)
              writeOut(unixEpoch, item)
              1
            case _ => 0
          }
        }
    }

  /** Executes the Digital Virginias harvest
    */
  override def localHarvest(): DataFrame = {
    val harvestTime = System.currentTimeMillis()
    val unixEpoch = harvestTime / 1000L
    val inFiles = new File(conf.harvest.endpoint.getOrElse("in"))

    inFiles
      .listFiles(zipFilter)
      .foreach(inFile => {
        val inputStream = FileHarvester.getZipInputStream(inFile)
          .getOrElse(
            throw new IllegalArgumentException("Couldn't load ZIP files.")
          )
        FileHarvester.iter(inputStream).foreach(result => {
          handleFile(result, unixEpoch) match {
            case Failure(exception) =>
              LogManager
                .getLogger(this.getClass)
                .error(s"Caught exception on $inFile.", exception)
            case Success(count) =>
              count
          }
        })
        IOUtils.closeQuietly(inputStream)
      })

    // flush the avroWriter
    flush()

    // Read harvested data into Spark DataFrame and return.
    spark.read.format("avro").load(tmpOutStr)
  }
}
