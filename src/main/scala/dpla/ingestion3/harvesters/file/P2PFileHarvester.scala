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
  * Entry for performing a CO-WY file harvest
  */
class P2PFileHarvester(shortName: String,
                       conf: i3Conf,
                       outputDir: String,
                       harvestLogger: Logger)
  extends FileHarvester(shortName, conf, outputDir, harvestLogger) {

  // Holds the output of handleXML
  case class JsonResult(id: String, itemJson: String)

  /**
    * Case class to hold the results of a ZipEntry.
    *
    * @param entryName Path of the entry in the tarfile
    * @param data      Holds the data for the entry, or None if it's a directory.
    */
  case class ZipResult(entryName: String, data: Option[Array[Byte]])

  protected val extractor = new P2PFileExtractor()

  private val logger = LogManager.getLogger(getClass)

  /**
    *
    *
    * @param schema     Parsed Avro schema
    * @param zipResult  Case class representing extracted item from the tar
    * @return Count of metadata items found.
    */
  def handleFile(schema: Schema,
                 zipResult: ZipResult,
                 unixEpoch: Long): Try[Int] = {
    zipResult.data match {
      case None =>
        Success(0) // a directory, no results
      case Some(data) =>
        Try {
          val json = parse(new String(data))
          val item = handleJson(json)
          val entryName = zipResult.entryName
          logger.info(s"Entry name: $entryName")

          item match {
            case Some(i) =>
              // println(i.id)
              val genericRecord = new GenericData.Record(schema)
              genericRecord.put("id", i.id)
              genericRecord.put("ingestDate", unixEpoch)
              genericRecord.put("provider", "p2p")
              genericRecord.put("document", i.itemJson)
              genericRecord.put("mimetype", "application_json")
              avroWriter.append(genericRecord)
              1
            case _ => 0
          }
        }
    }
  }

  /**
    * Takes
    *
    * @param json Root of the xml document
    * @return List of Options of id/item pairs.
    */
  def handleJson(json: JValue): Option[JsonResult] =
  // TODO Ensure we are consistantly extracting the same ID for a record (filename might be a safer option?)
    Option(JsonResult(
      extractor.extractStrings(json \ "@graph" \ "@id").find(_.startsWith("https://plains2peaks.org/"))
        .getOrElse(throw new RuntimeException("Missing ID")), compact(render(json))))

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

  protected def localFileHarvest: Unit = {
    val startTime = System.currentTimeMillis()
    val unixEpoch = startTime / 1000L
    val inFile = new File(conf.harvest.endpoint.getOrElse("in"))

    inFile.listFiles(new ZipFileFilter).foreach( inFile => {
      val inputStream = getInputStream(inFile)
        .getOrElse(throw new IllegalArgumentException("Couldn't load ZIP files."))
      for (zipResult <- iter(inputStream)) yield {
        handleFile(schema, zipResult, unixEpoch) match {
          case Failure(exception) =>
            logger.error(s"Caught exception on $inFile.", exception)
          case Success(_) =>
        }
      }
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