package dpla.ingestion3.harvesters.file

import java.io.{File, FileFilter, FileInputStream}
import java.util.zip.ZipInputStream

import dpla.ingestion3.mappers.utils.JsonExtractor
import dpla.ingestion3.utils.{AvroUtils, FlatFileIO, Utils}
import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.commons.io.IOUtils
import org.apache.log4j.LogManager
import org.json4s.jackson.JsonMethods._
import org.json4s.{JValue, _}
import org.rogach.scallop.{ScallopConf, ScallopOption}

import scala.util.{Failure, Success, Try}


/**
  * FileFilter to filter out non-Zip files
  */
class ZipFileFilter extends FileFilter {
  override def accept(pathname: File): Boolean = pathname.getName.endsWith("zip")
}

/**
  * Extracts values from parsed JSON
  */
class CoWyFileExtractor extends JsonExtractor

/**
  * Entry for performing a CO-WY file harvest
  */
object CoWyFileHarvester {

  protected val extractor = new CoWyFileExtractor()

  private val logger = LogManager.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()
    val unixEpoch = startTime / 1000L

    val conf = new CoWyFileHarvestConf(args)
    val inFile = new File(conf.inputFile.getOrElse("in"))
    val outFile = new File(conf.outputFile.getOrElse("out"))

    Utils.deleteRecursively(outFile)

    val schemaStr = new FlatFileIO().readFileAsString("/avro/OriginalRecord.avsc")
    val schema = new Schema.Parser().parse(schemaStr)
    val avroWriter = AvroUtils.getAvroWriter(outFile, schema)

     inFile.listFiles(new ZipFileFilter).foreach( inFile => {
       val inputStream = getInputStream(inFile)
        .getOrElse(throw new IllegalArgumentException("Couldn't load ZIP files."))

      for (zipResult <- iter(inputStream)) yield {
        handleFile(schema, avroWriter, zipResult, unixEpoch) match {
          case Failure(exception) =>
            logger.error(s"Caught exception on $inFile.", exception)
            0
          case Success(count) =>
            count
        }
      }
      IOUtils.closeQuietly(inputStream)
    })
    avroWriter.close()
  }

  /**
    * Main logic for handling individual entries in the tar.
    *
    * @param schema     Parsed Avro schema
    * @param avroWriter Writer for saving Avros
    * @param zipResult  Case class representing extracted item from the tar
    * @return Count of metadata items found.
    */
  def handleFile(schema: Schema, avroWriter: DataFileWriter[GenericRecord], zipResult: ZipResult, unixEpoch: Long) =
    zipResult.data match {
      case None =>
        Success(0) //a directory, no results

      case Some(data) =>
        Try {
          val json = parse(new String(data))
          val item = handleJson(json)
          val entryName = zipResult.entryName
          logger.info(entryName)

          item match {
            case Some(i) =>
              println(i.id)
              val genericRecord = new GenericData.Record(schema)
              genericRecord.put("id", i.id)
              genericRecord.put("ingestDate", unixEpoch)
              genericRecord.put("provider", "CO-WY")
              genericRecord.put("document", i.itemJson)
              genericRecord.put("mimetype", "application_json")
              avroWriter.append(genericRecord)
              1
            case _ => 0
          }
        }
    }

  /**
    * Takes care of parsing an xml file into a list of Nodes each representing an item
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
    println(file)
    file.getName match {
      case zipName if zipName.endsWith("zip") =>
        Some(new ZipInputStream(new FileInputStream(file)))
      case _ => None
    }
  }

  /**
    * Implements a stream of files from the tar.
    * Can't use @tailrec here because the compiler can't recognize it as tail recursive,
    * but this won't blow the stack.
    *
    * @param zipInputStream
    * @return Lazy stream of tar recordsd
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
    * Holds the output of handleXML
    */
  case class JsonResult(id: String, itemJson: String)

  /**
    * Case class to hold the results of a TarEntry.
    *
    * @param entryName Path of the entry in the tarfile
    * @param data      Holds the data for the entry, or None if it's a directory.
    */
  case class ZipResult(entryName: String, data: Option[Array[Byte]])
}

class CoWyFileHarvestConf(arguments: Seq[String]) extends ScallopConf(arguments) {

  val inputFile: ScallopOption[String] = opt[String](
    "inputFile",
    required = true,
    noshort = true,
    validate = _.nonEmpty
  )

  val outputFile: ScallopOption[String] = opt[String](
    "outputFile",
    required = true,
    noshort = true,
    validate = _.nonEmpty
  )

  verify()
}