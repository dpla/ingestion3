package dpla.ingestion3.harvesters.file

import java.io.{File, FileFilter, FileInputStream}
import java.util.zip.ZipInputStream

import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.mappers.utils.JsonExtractor
import org.apache.avro.generic.GenericData
import org.apache.commons.io.IOUtils
import org.apache.log4j.Logger
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
                       logger: Logger)
  extends FileHarvester(shortName, conf, outputDir, logger) {

  protected val extractor = new P2PFileExtractor()

  override protected val mimeType: String = "application_json"

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
    * Parses JValue to extract item local item id and renders compact
    * full record
    *
    * @param json Full JSON item record
    * @return Option[ParsedResult]
    */
  def getJsonResult(json: JValue): Option[ParsedResult] =
    Option(ParsedResult(
      // item id
      // TODO Are we consistently extracting the same ID for a record? Are filenames a safer option?
      extractor.extractStrings(json \ "@graph" \ "@id").find(_.startsWith("https://plains2peaks.org/"))
        .getOrElse(throw new RuntimeException("Missing ID")),
      // item
      compact(render(json))
    ))

  /**
    * Parses and extracts ZipInputStream and writes
    * parsed records out.
    *
    * @param zipResult  Case class representing extracted items from the zip
    * @return Count of metadata items found.
    */
  def handleFile(zipResult: FileResult,
                 unixEpoch: Long): Try[Int] =

    zipResult.data match {
      case None =>
        Success(0) // a directory, no results
      case Some(data) => Try {
        Try {
          parse(new String(data))
        } match {
          case Success(json) =>
            getJsonResult(json) match {
              case Some(item) =>
                val genericRecord = new GenericData.Record(schema)
                genericRecord.put("id", item.id)
                genericRecord.put("ingestDate", unixEpoch)
                genericRecord.put("provider", "p2p")
                genericRecord.put("document", item.item)
                genericRecord.put("mimetype", mimeType)
                avroWriter.append(genericRecord)
                1
              case _ => 0
            }
          case _ => 0
        }
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
  def iter(zipInputStream: ZipInputStream): Stream[FileResult] =
    Option(zipInputStream.getNextEntry) match {
      case None =>
        Stream.empty
      case Some(entry) =>
        val result =
          if (entry.isDirectory)
            None
          else
            Some(IOUtils.toByteArray(zipInputStream, entry.getSize))
        FileResult(entry.getName, result) #:: iter(zipInputStream)
    }

  /**
    * Executes the plains2peaks harvest
    */
  protected def localHarvest(): Unit = {
    val harvestTime = System.currentTimeMillis()
    val unixEpoch = harvestTime / 1000L
    val inFiles = new File(conf.harvest.endpoint.getOrElse("in"))

    inFiles.listFiles(new ZipFileFilter).foreach( inFile => {
      val inputStream = getInputStream(inFile)
        .getOrElse(throw new IllegalArgumentException("Couldn't load ZIP files."))
      val recordCount = (for (result <- iter(inputStream)) yield {
        handleFile(result, unixEpoch) match {
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