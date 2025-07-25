package dpla.ingestion3.harvesters.file

import java.io.{BufferedReader, File, FileInputStream}
import java.util.zip.GZIPInputStream
import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.harvesters.file.FileFilters.{avroFilter, gzFilter, xmlFilter}
import dpla.ingestion3.harvesters.{AvroHelper, FileResult, Harvester, LocalHarvester, ParsedResult}
import dpla.ingestion3.model.AVRO_MIME_XML
import dpla.ingestion3.utils.{FlatFileIO, Utils}
import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.hadoop.fs.Path
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.tools.bzip2.CBZip2InputStream
import org.apache.tools.tar.TarInputStream

import scala.util.{Failure, Success, Try}
import scala.xml._

class NaraFileHarvester(
    spark: SparkSession,
    shortName: String,
    conf: i3Conf
) extends LocalHarvester(shortName, conf) {

  lazy val naraSchema: Schema =
    new Schema.Parser()
      .parse(new FlatFileIO().readFileAsString("/avro/NaraOriginalRecord.avsc"))

  val avroWriterNara: DataFileWriter[GenericRecord] =
    AvroHelper.avroWriter(shortName, naraTmp, naraSchema)

  // Temporary output path.
  lazy val naraTmp: String =
    new File(FileUtils.getTempDirectory, shortName).getAbsolutePath

  def mimeType: GenericData.EnumSymbol = AVRO_MIME_XML

  /** Loads .gz, .tgz, .bz, and .tbz2, and plain old .tar files.
    *
    * @param file
    *   File to parse
    * @return
    *   TarInputstream of the tar contents
    */
  def getInputStream(file: File): Option[TarInputStream] =
    file.getName match {
      case gzName if gzName.endsWith("gz") || gzName.endsWith("tgz") =>
        Some(new TarInputStream(new GZIPInputStream(new FileInputStream(file))))

      case bz2name if bz2name.endsWith("bz2") || bz2name.endsWith("tbz2") =>
        val inputStream = new FileInputStream(file)
        inputStream.skip(2)
        Some(new TarInputStream(new CBZip2InputStream(inputStream)))

      case tarName if tarName.endsWith("tar") =>
        Some(new TarInputStream(new FileInputStream(file)))

      case _ => None
    }

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
        val id =
          (record \ "digitalObjectArray" \ "digitalObject" \ "objectIdentifier").text
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
    genericRecord.put(
      "filename",
      file
    ) // Used to order records by date updated.
    genericRecord.put("mimetype", mimeType)

    avroWriter.append(genericRecord)

  }

  /** Implements a stream of files from the tar. Can't use @tailrec here because
    * the compiler can't recognize it as tail recursive, but this won't blow the
    * stack.
    *
    * @param tarInputStream
    * @return
    *   Lazy stream of tar records
    */
  def iter(tarInputStream: TarInputStream): LazyList[FileResult] =
    Option(tarInputStream.getNextEntry) match {
      case None =>
        LazyList.empty

      case Some(entry) =>
        val filename = Try {
          entry.getName
        }.getOrElse("")

        val result =
          if (
            entry.isDirectory || filename.contains("._")
          ) // drop OSX hidden files
            None
          else if (filename.endsWith(".xml")) // only read xml files
            Some(IOUtils.toByteArray(tarInputStream, entry.getSize))
          else
            None

        FileResult(entry.getName, result) #:: iter(tarInputStream)
    }

  /** Executes the nara harvest
    */
  override def harvest: DataFrame = {
    val harvestTime = System.currentTimeMillis()
    val unixEpoch = harvestTime / 1000L

    val logger = LogManager.getLogger(this.getClass)

    logger.info(s"Writing harvest tmp output to $naraTmp")


    val inFile = new File(conf.harvest.endpoint.getOrElse("in"))

    val deletes = new File(inFile, "/deletes/")

    if (inFile.isDirectory)
      for (file: File <- inFile.listFiles(gzFilter).sorted) {
        logger.info(s"Harvesting from ${file.getName}")
        harvestFile(file, unixEpoch)
      }
    else
      harvestFile(inFile, unixEpoch)

    // flush writes
    avroWriterNara.flush()

    // Get the absolute path of the avro file written to naraTmp directory. copyFromLocalFile() cannot copy
    // a directory
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
    val dfAllRecords = spark.read.format("avro").load(localSrcPath.toString)

    import spark.implicits._
    val w = Window.partitionBy($"id").orderBy($"filename".desc)

    val df = dfAllRecords
      .withColumn("rn", row_number.over(w))
      .where($"rn" === 1)
      .drop("rn")

    // process deletes
    if (deletes.exists()) {
      val del = getIdsToDelete(deletes)
      logger.info(
        s"${del.count()} records to delete in ${deletes.getAbsolutePath}"
      )
      df.where(!col("id").isin(del))
    } else {
      logger.info(
        "No deletes files specified. Removing no records from NARA data export"
      )
      df
    }
  }

  /** Collect IDs from file(s) to be deleted from NARA harvest. This does not
    * currently support adding records back which had previously been deleted.
    *
    * @param file
    *   File Path to file or folder the defines which NARA IDs should be deleted
    * @return
    *   DataFrame IDs to delete
    */
  def getIdsToDelete(file: File): DataFrame = {
    import spark.implicits._

    val files =
      if (file.isDirectory) file.listFiles(xmlFilter).sorted
      else Array(file)

    files
      .flatMap(file => {
        val xml = XML.loadFile(file)
        (xml \\ "naId").map(_.text)
      })
      .distinct
      .toSeq
      .toDF()
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
    val inputStream = getInputStream(file)
      .getOrElse(
        throw new IllegalArgumentException(
          s"Couldn't load tar file: ${file.getAbsolutePath}"
        )
      )

    LocalHarvester.iter(inputStream).foreach(tarResult =>
      handleFile(tarResult, unixEpoch, file.getName) match {
        case Failure(exception) =>
          val logger = LogManager.getLogger(this.getClass)
          logger
            .error(s"Caught exception on ${tarResult.entryName}.", exception)

        case Success(count) => ()
      }
    )

    IOUtils.closeQuietly(inputStream)
  }


}
