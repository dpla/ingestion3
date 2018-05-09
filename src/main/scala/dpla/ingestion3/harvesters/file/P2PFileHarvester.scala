package dpla.ingestion3.harvesters.file

import java.io.{File, FileFilter, FileInputStream}
import java.util.zip.ZipInputStream

import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.mappers.utils.JsonExtractor
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.commons.io.IOUtils
import org.apache.log4j.{LogManager, Logger}
import org.json4s.jackson.JsonMethods._
import org.json4s.{JValue, _}

import scala.util.{Failure, Success, Try}


/**
  * Extracts values from parsed JSON
  */
class P2PFileExtractor extends JsonExtractor

/**
  * Entry for performing a plains2peaks file harvest
  */
class P2PFileHarvester(shortName: String,
                       conf: i3Conf,
                       outputDir: String,
                       harvestLogger: Logger)
  extends FileHarvester(shortName, conf, outputDir, harvestLogger) {

  /**
    * Holds the output of handleJson
    */
  case class JsonResult(id: String, itemJson: String)

  /**
    * Case class to hold the results of a ZipEntry.
    *
    * @param entryName Path of the entry in the zipfile
    * @param data      Holds the data for the entry, or None if it's a directory.
    */
  case class ZipResult(entryName: String, data: Option[Array[Byte]])

  protected val extractor = new P2PFileExtractor()

  private val logger = LogManager.getLogger(getClass)

  /**
    * Parses and extracts ZipInputStream and writes
    * parses records out.
    *
    * @param schema     Parsed Avro schema
    * @param zipResult  Case class representing extracted items from the zip
    * @return Count of metadata items found.
    */
  def handleFile(schema: Schema,
                 zipResult: ZipResult,
                 unixEpoch: Long): Try[Int] =

    zipResult.data match {
      case None =>
        Success(0) // a directory, no results
      case Some(data) =>
        Try {
          val json = parse(new String(data))
          val entryName = zipResult.entryName

          handleJson(json) match {
            case Some(item) =>
              val genericRecord = new GenericData.Record(schema)
              genericRecord.put("id", item.id)
              genericRecord.put("ingestDate", unixEpoch)
              genericRecord.put("provider", "p2p")
              genericRecord.put("document", item.itemJson)
              genericRecord.put("mimetype", "application_json")
              avroWriter.append(genericRecord)
              1
            case _ => 0
          }
        }
    }

  /**
    * Parses JValue to extract item local item id and renders compact
    * full record
    *
    * @param json Full JSON item record
    * @return Option[JsonResult]
    */
  def handleJson(json: JValue): Option[JsonResult] =
    Option(JsonResult(
      // item id
      // TODO Are we consistently extracting the same ID for a record? Are filenames a safer option?
      extractor.extractStrings(json \ "@graph" \ "@id").find(_.startsWith("https://plains2peaks.org/"))
        .getOrElse(throw new RuntimeException("Missing ID")),
      // item
      compact(render(json))
    ))

  /**
    * Loads .zip files
    *
    * @param file File to parse
    * @return TarInputstream of the tar contents
    */
  def getInputStream(file: File): Option[ZipInputStream] = {
    file.getName match {
      case zipName if zipName.endsWith("zip") =>
        Some(new ZipInputStream(new FileInputStream(file)))
      case _ => None
    }
  }

  /**
    * Implements a stream of files from the zip
    * Can't use @tailrec here because the compiler can't recognize it as tail recursive,
    * but this won't blow the stack.
    *
    * @param zipInputStream
    * @return Lazy stream of tar records
    */
  def iter(zipInputStream: ZipInputStream): Stream[ZipResult] =
    Option(zipInputStream.getNextEntry) match {
      case None =>
        Stream.empty
      case Some(entry) =>
        val result =
          if (entry.isDirectory)
            None
          else
            Some(IOUtils.toByteArray(zipInputStream, entry.getSize))
        ZipResult(entry.getName, result) #:: iter(zipInputStream)
    }

  /**
    * Abstract method mimeType should store the mimeType of the harvested data.
    */
  override protected val mimeType: String = "application_json"

  /**
    * Executes the plains2peaks harvest
    */
  protected def localFileHarvest: Unit = {
    val startTime = System.currentTimeMillis()
    val unixEpoch = startTime / 1000L
    val inFiles = new File(conf.harvest.endpoint.getOrElse("in"))

    inFiles.listFiles(new ZipFileFilter).foreach( inFile => {
      val inputStream = getInputStream(inFile)
        .getOrElse(throw new IllegalArgumentException("Couldn't load ZIP files."))

      val recordCount = (for (zipResult <- iter(inputStream)) yield {
        handleFile(schema, zipResult, unixEpoch) match {
          case Failure(exception) =>
            logger.error(s"Caught exception on $inFile.", exception)
            0
          case Success(count) =>
            count
        }
      }).sum
      IOUtils.closeQuietly(inputStream)
    })
  }
}


/**
  * FileFilter to filter out non-Zip files
  */
class ZipFileFilter extends FileFilter {
  override def accept(pathname: File): Boolean = pathname.getName.endsWith("zip")
}