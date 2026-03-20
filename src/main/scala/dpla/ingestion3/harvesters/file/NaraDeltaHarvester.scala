package dpla.ingestion3.harvesters.file

import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.harvesters.file.FileFilters.{avroFilter, gzFilter, zipFilter}
import dpla.ingestion3.harvesters.{AvroHelper, FileResult, Harvester, LocalHarvester, ParsedResult}
import dpla.ingestion3.model.AVRO_MIME_XML
import dpla.ingestion3.utils.{FlatFileIO, Utils}
import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.{File, FileFilter}
import scala.util.{Failure, Success, Try, Using}
import scala.xml._

class NaraDeltaHarvester(
    spark: SparkSession,
    shortName: String,
    conf: i3Conf
) extends LocalHarvester(shortName, conf) {

  val naraSchema: Schema =
    new Schema.Parser()
      .parse(new FlatFileIO().readFileAsString("/avro/OriginalRecord.avsc"))

  val logger: Logger = LogManager.getLogger(this.getClass)

  val avroWriterNara: DataFileWriter[GenericRecord] =
    AvroHelper.avroWriter(shortName, naraTmp, naraSchema)

  // Temporary output path -- use a unique subdirectory to avoid conflict
  // with the parent LocalHarvester's tmpOutStr which resolves to the same path.
  lazy val naraTmp: String =
    new File(new File(FileUtils.getTempDirectory, shortName), "delta").getAbsolutePath

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
            writeOutNara(unixEpoch, item)
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
  def writeOutNara(unixEpoch: Long, item: ParsedResult): Unit = {
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

  /** Combined file filter matching both .gz and .zip files */
  private val gzOrZipFilter: FileFilter = f => gzFilter.accept(f) || zipFilter.accept(f)

  /** If the path is an S3 URI, syncs the prefix to a local temp directory and
    * returns that directory. Otherwise returns the path as a local File.
    *
    * @param path
    *   Local path or s3:// URI
    * @param harvestTime
    *   Timestamp used to make the temp directory name unique
    * @return
    *   Local directory containing the downloaded files
    */
  private def resolveToLocalDir(path: String, harvestTime: Long): File = {
    if (path.startsWith("s3://"))
      logger.info(s"Syncing NARA source from S3: $path")
    LocalHarvester.resolveToLocalDir(path, harvestTime, "nara-s3", conf.harvest.awsProfile)
  }

  /** Executes the nara harvest
    */
  def harvest: DataFrame = {
    val harvestTime = System.currentTimeMillis()
    val unixEpoch = harvestTime / 1000L

    // Path to the incremental update files (local directory or S3 prefix)
    val deltaIn = conf.harvest.update.getOrElse("in")
    val isTempDir = deltaIn.startsWith("s3://")
    val deltaHarvestInFile = resolveToLocalDir(deltaIn, harvestTime)

    try {
      if (deltaHarvestInFile.isDirectory)
        for (file: File <- Option(deltaHarvestInFile.listFiles(gzOrZipFilter)).getOrElse(Array.empty).sorted) {
          logger.info(s"Harvesting NARA delta changes from ${file.getName}")
          harvestFile(file, unixEpoch)
        }
      else
        harvestFile(deltaHarvestInFile, unixEpoch)
    } finally {
      if (isTempDir) FileUtils.deleteQuietly(deltaHarvestInFile)
    }

    // Close the avro writer to ensure the file footer and sync markers are
    // written. Spark's avro reader requires a properly closed file.
    avroWriterNara.close()

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

    val tempFileSize = new File(naraTempFile).length()
    logger.info(s"Reading harvested records from $naraTempFile (size: $tempFileSize bytes)")
    val localSrcPath = new Path(naraTempFile)
    val dfDeltaRecords = spark.read.format("avro").load(localSrcPath.toString)
    val tempCount = dfDeltaRecords.count()
    logger.info(s"Spark read $tempCount records from temp avro file")

    dfDeltaRecords
  }

  override def cleanUp(): Unit = {
    logger.info(s"Cleaning up $naraTmp directory and files")
    // Writer may already be closed by harvest() -- safe to call again
    Try(avroWriterNara.close())
    // Delete temporary output directory and files.
    Utils.deleteRecursively(new File(naraTmp))
  }

  private def processEntries(entries: LazyList[FileResult], unixEpoch: Long): Int =
    entries.map(result =>
      handleFile(result, unixEpoch) match {
        case Failure(exception) =>
          logger.error(s"Caught exception on ${result.entryName}.", exception)
          0
        case Success(count) => count
      }
    ).sum

  private def harvestFile(file: File, unixEpoch: Long): Unit = {
    val result =
      if (file.getName.endsWith(".zip")) {
        Using(
          LocalHarvester.getZipInputStream(file).getOrElse(
            throw new IllegalArgumentException(s"Couldn't load ZIP: ${file.getAbsolutePath}")
          )
        ) { inputStream =>
          processEntries(LocalHarvester.iter(inputStream), unixEpoch)
        }
      } else {
        Using(
          LocalHarvester.getTarInputStream(file).getOrElse(
            throw new IllegalArgumentException(s"Couldn't load tar file: ${file.getAbsolutePath}")
          )
        ) { inputStream =>
          processEntries(LocalHarvester.iter(inputStream), unixEpoch)
        }
      }

    result match {
      case Failure(exception) =>
        logger.error(s"Failed to process file: ${file.getName}", exception)
      case Success(count) =>
        logger.info(s"Harvested $count records from ${file.getName}")
    }
  }
}
