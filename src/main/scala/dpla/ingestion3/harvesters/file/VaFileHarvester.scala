package dpla.ingestion3.harvesters.file

import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.harvesters.file.FileFilters.zipFilter
import dpla.ingestion3.harvesters.oai.LocalOaiHarvester
import dpla.ingestion3.harvesters.{FileResult, Harvester, LocalHarvester, ParsedResult}
import dpla.ingestion3.mappers.utils.XmlExtractor
import dpla.ingestion3.model.AVRO_MIME_XML
import org.apache.avro.generic.GenericData
import org.apache.commons.io.IOUtils
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File
import scala.util.{Failure, Success, Try}
import scala.xml.XML

/** Entry for performing a Digital Virginias file harvest
  */
class VaFileHarvester(
    spark: SparkSession,
    shortName: String,
    conf: i3Conf
) extends LocalHarvester(shortName, conf) with XmlExtractor {

  def mimeType: GenericData.EnumSymbol = AVRO_MIME_XML

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
              val item = ParsedResult(id, outputXml)
              writeOut(unixEpoch, item)
              1
            case _ => 0
          }
        }
    }

  /** Executes the Digital Virginias harvest
    */
  override def harvest: DataFrame = {
    val harvestTime = System.currentTimeMillis()
    val unixEpoch = harvestTime / 1000L
    val inFiles = new File(conf.harvest.endpoint.getOrElse("in"))

    inFiles
      .listFiles(zipFilter)
      .foreach(inFile => {
        val inputStream = LocalHarvester.getZipInputStream(inFile)
          .getOrElse(
            throw new IllegalArgumentException("Couldn't load ZIP files.")
          )
        LocalHarvester.iter(inputStream).foreach(result => {
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

    close()

    // Read harvested data into Spark DataFrame and return.
    spark.read.format("avro").load(tmpOutStr)
  }
}
