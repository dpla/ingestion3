package dpla.ingestion3.harvesters.file

import java.io.{File, FileInputStream}
import java.util.zip.GZIPInputStream

import dpla.ingestion3.utils.{FlatFileIO, Utils}
import org.apache.avro.Schema
import org.apache.avro.file.{CodecFactory, DataFileWriter}
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.commons.io.IOUtils
import org.apache.log4j.LogManager
import org.apache.tools.bzip2.CBZip2InputStream
import org.apache.tools.tar.TarInputStream
import org.rogach.scallop.{ScallopConf, ScallopOption}

import scala.util.{Failure, Success, Try}
import scala.xml.{MinimizeMode, Node, Utility, XML}


object NaraFileHarvestMain {


  private val logger = LogManager.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()
    val unixEpoch = startTime / 1000L
    val conf = new NaraFileHarvestConf(args)
    val inFile = new File(conf.inputFile.getOrElse("in"))
    val outFile = new File(conf.outputFile.getOrElse("out"))
    Utils.deleteRecursively(outFile)
    val schemaStr = new FlatFileIO().readFileAsString("/avro/OriginalRecord.avsc")
    val schema = new Schema.Parser().parse(schemaStr)
    val avroWriter = getAvroWriter(outFile, schema)

    val inputStream = getInputStream(inFile)
      .getOrElse(throw new IllegalArgumentException("Couldn't load tar file."))

    val recordCount = (for (tarResult <- iter(inputStream)) yield {
      handleFile(schema, avroWriter, tarResult, unixEpoch) match {
        case Failure(exception) =>
          logger.error(s"Caught exception on $inFile.", exception)
          0

        case Success(count) =>
          count
      }
    }).sum

    avroWriter.close()
    IOUtils.closeQuietly(inputStream)

    val endTime = System.currentTimeMillis()
    Utils.printResults(endTime - startTime, recordCount)
  }

  /**
    * Main logic for handling individual entries in the tar.
    *
    * @param schema     Parsed Avro schema
    * @param avroWriter Writer for saving Avros
    * @param tarResult  Case class representing extracted item from the tar
    * @return Count of metadata items found.
    */
  def handleFile(schema: Schema, avroWriter: DataFileWriter[GenericRecord], tarResult: TarResult, unixEpoch: Long): Try[Int] =
    tarResult.data match {
      case None =>
        Success(0) //a directory, no results

      case Some(data) =>
        Try {
          val xml = XML.loadString(new String(data))
          val items = handleXML(xml)
          val entryName = tarResult.entryName

          val counts = for {
            itemOption <- items
            item <- itemOption //filters out the Nones
          } yield {
            val genericRecord = new GenericData.Record(schema)
            genericRecord.put("id", item.id)
            genericRecord.put("ingestDate", unixEpoch)
            genericRecord.put("provider", "NARA")
            genericRecord.put("document", item.itemXml)
            genericRecord.put("mimetype", "application_xml")
            avroWriter.append(genericRecord)
            1
          }

          counts.sum
        }
    }

  /**
    * Takes care of parsing an xml file into a list of Nodes each representing an item
    *
    * @param xml Root of the xml document
    * @return List of Options of id/item pairs.
    */
  def handleXML(xml: Node): List[Option[XmlResult]] =
    for {
    //three different types of nodes contain children that represent records
      items <- xml \\ "item" :: xml \\ "itemAv" :: xml \\ "fileUnit" :: Nil
      item <- items
      if (item \ "digitalObjectArray" \ "digitalObject").nonEmpty
    } yield item match {
      case record: Node =>
        val id = (record \ "digitalObjectArray" \ "digitalObject" \ "objectIdentifier").text.toString
        val outputXML = xmlToString(record)
        val label = item.label
        Some(XmlResult(id, outputXML))
      case _ =>
        logger.warn("Got weird result back for item path: " + item.getClass)
        None
    }

  /**
    * Converts a Node to an xml string
    *
    * @param node The root of the tree to write to a string
    * @return a String containing xml
    */
  def xmlToString(node: Node): String =
    Utility.serialize(node, minimizeTags = MinimizeMode.Always).toString

  /**
    * Builds a writer for saving Avros.
    *
    * @param outputFile Place to save Avro
    * @param schema     Parsed schema of the output
    * @return DataFileWriter for writing Avros in the given schema
    */
  def getAvroWriter(outputFile: File, schema: Schema): DataFileWriter[GenericRecord] = {
    val datumWriter = new GenericDatumWriter[GenericRecord](schema)
    val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
    dataFileWriter.setCodec(CodecFactory.deflateCodec(1))
    dataFileWriter.setSyncInterval(1024 * 1024 * 2) //2M
    dataFileWriter.create(schema, outputFile)
  }

  /**
    * Loads .gz, .tgz, .bz, and .tbz2, and plain old .tar files.
    *
    * @param file File to parse
    * @return TarInputstream of the tar contents
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

  /**
    * Implements a stream of files from the tar.
    * Can't use @tailrec here because the compiler can't recognize it as tail recursive,
    * but this won't blow the stack.
    *
    * @param tarInputStream
    * @return Lazy stream of tar recordsd
    */
  def iter(tarInputStream: TarInputStream): Stream[TarResult] =
    Option(tarInputStream.getNextEntry) match {
      case None =>
        Stream.empty

      case Some(entry) =>
        val result =
          if (entry.isDirectory)
            None
          else
            Some(IOUtils.toByteArray(tarInputStream, entry.getSize))
        TarResult(entry.getName, result) #:: iter(tarInputStream)
    }

  /**
    * Holds the output of handleXML
    */
  case class XmlResult(id: String, itemXml: String)

  /**
    * Case class to hold the results of a TarEntry.
    *
    * @param entryName Path of the entry in the tarfile
    * @param data      Holds the data for the entry, or None if it's a directory.
    */
  case class TarResult(entryName: String, data: Option[Array[Byte]])

}

class NaraFileHarvestConf(arguments: Seq[String]) extends ScallopConf(arguments) {

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