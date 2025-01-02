package dpla.ingestion3.harvesters.file

import java.io.{File, FileInputStream}
import java.util.zip.GZIPInputStream
import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.harvesters.file.FileFilters.{avroFilter, gzFilter}
import dpla.ingestion3.harvesters.{AvroHelper, Harvester, LocalHarvester}
import dpla.ingestion3.model.AVRO_MIME_XML
import dpla.ingestion3.utils.{FlatFileIO, Utils}
import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.hadoop.fs.Path
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.tools.bzip2.CBZip2InputStream
import org.apache.tools.tar.TarInputStream

import scala.util.{Failure, Success, Try}
import scala.xml._

class NaraDeltaHarvester(
    spark: SparkSession,
    shortName: String,
    conf: i3Conf
) extends LocalHarvester(spark, shortName, conf) {


  lazy val naraSchema: Schema =
    new Schema.Parser()
      .parse(new FlatFileIO().readFileAsString("/avro/OriginalRecord.avsc"))

  val avroWriterNara: DataFileWriter[GenericRecord] =
    AvroHelper.avroWriter(shortName, naraTmp, naraSchema)

  // Temporary output path.
  lazy val naraTmp: String =
    new File(FileUtils.getTempDirectory, shortName).getAbsolutePath

  def mimeType: GenericData.EnumSymbol = AVRO_MIME_XML


  /** Takes care of parsing an xml file into a list of Nodes each representing
    * an item
    *
    * @param xml
    *   Root of the xml document
    * @return
    *   List of Options of id/item pairs.
    */
  def handleXML(xml: Node): List[Option[ParsedResult]] = {
    for {
      // three different types of nodes contain children that represent records
      items <- xml \\ "item" :: xml \\ "itemAv" :: xml \\ "fileUnit" :: Nil
      item <- items
      if (item \ "digitalObjectArray" \ "digitalObject").nonEmpty
    } yield item match {
      case record: Node =>
        val id = (record \ "naId").text
        val outputXML = Harvester.xmlToString(record)
        Some(ParsedResult(id, outputXML))
      case _ =>
        val logger = LogManager.getLogger(this.getClass)
        logger.warn("Got weird result back for item path: " + item.getClass)
        None
    }
  }

  /** Main logic for handling individual entries in the tar.
    *
    * @param tarResult
    *   Case class representing extracted item from the tar
    * @return
    *   Count of metadata items found.
    */
  def handleFile(
      tarResult: FileResult,
      unixEpoch: Long,
      filename: String
  ): Try[Int] =
    tarResult.data match {
      case None =>
        Success(0) // a directory, no results

      case Some(data) =>
        Try {
          val dataString = new String(data).replaceAll("<\\?xml.*\\?>", "").trim
          val xml = XML.loadString(dataString)

          val items = handleXML(xml)

          val counts = for {
            itemOption <- items
            item <- itemOption // filters out the Nones
          } yield {
            writeOutNara(unixEpoch, item, filename)
            1
          }
          counts.sum
        }
    }

  def getAvroWriterNara: DataFileWriter[GenericRecord] = avroWriterNara

  /** Writes item out
    *
    * @param unixEpoch
    *   Timestamp of the harvest
    * @param item
    *   Harvested record
    */
  def writeOutNara(unixEpoch: Long, item: ParsedResult, file: String): Unit = {
    val avroWriter = getAvroWriterNara
    val schema = naraSchema

    val genericRecord = new GenericData.Record(schema)
    genericRecord.put("id", item.id)
    genericRecord.put("ingestDate", unixEpoch)
    genericRecord.put("provider", shortName)
    genericRecord.put("document", item.item)
    genericRecord.put("mimetype", mimeType)

    avroWriter.append(genericRecord)

  }


  /** Executes the nara harvest
    */
  def localHarvest(): DataFrame = {
    val harvestTime = System.currentTimeMillis()
    val unixEpoch = harvestTime / 1000L

    // Path to the incremental update file [*.tar.gz without deletes]
    val deltaIn = conf.harvest.update.getOrElse("in")
    val deltaHarvestInFile = new File(deltaIn)

    if (deltaHarvestInFile.isDirectory)
      for (
        file: File <- deltaHarvestInFile.listFiles(gzFilter).sorted
      ) {
        val logger = LogManager.getLogger(this.getClass)
        logger.info(s"Harvesting NARA delta changes from ${file.getName}")
        harvestFile(file, unixEpoch)
      }
    else
      harvestFile(deltaHarvestInFile, unixEpoch)

    // flush writes
    avroWriterNara.flush()

    // Get the absolute path of the avro file written to naraTmp directory
    val naraTempFile = new File(naraTmp)
      .listFiles(avroFilter)
      .headOption
      .getOrElse(
        throw new RuntimeException(
          s"Unable to load avro file in $naraTmp directory. Unable to continue"
        )
      )
      .getAbsolutePath

    val localSrcPath = new Path(naraTempFile)
    val dfDeltaRecords = spark.read.format("avro").load(localSrcPath.toString)

    dfDeltaRecords
  }

  override def cleanUp(): Unit = {
    val logger = LogManager.getLogger(this.getClass)
    logger.info(s"Cleaning up $naraTmp directory and files")
    avroWriterNara.flush()
    avroWriterNara.close()
    // Delete temporary output directory and files.
    Utils.deleteRecursively(new File(naraTmp))
  }

  private def harvestFile(file: File, unixEpoch: Long): Unit = {
    val logger = LogManager.getLogger(this.getClass)
    val inputStream = FileHarvester.getTarInputStream(file)
      .getOrElse(
        throw new IllegalArgumentException(
          s"Couldn't load tar file: ${file.getAbsolutePath}"
        )
      )

    val recordCount = FileHarvester.iter(inputStream).map(tarResult =>
      handleFile(tarResult, unixEpoch, file.getName) match {
        case Failure(exception) =>
          val logger = LogManager.getLogger(this.getClass)
          logger
            .error(s"Caught exception on ${tarResult.entryName}.", exception)
          0
        case Success(count) =>
          count
      }
    ).sum

    logger.info(s"Harvested $recordCount records from ${file.getName}")

    IOUtils.closeQuietly(inputStream)
  }

}
