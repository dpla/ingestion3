package dpla.ingestion3.harvesters.file

import java.io.{File, FileInputStream}
import java.util.zip.GZIPInputStream

import dpla.ingestion3.confs.i3Conf
import org.apache.avro.generic.GenericData
import org.apache.commons.io.IOUtils
import org.apache.log4j.Logger
import org.apache.tools.bzip2.CBZip2InputStream
import org.apache.tools.tar.TarInputStream

import scala.util.{Failure, Success, Try}
import scala.xml.{MinimizeMode, Node, Utility, XML}


class NaraFileHarvester(shortName: String,
                      conf: i3Conf,
                      outputDir: String,
                      logger: Logger)
  extends FileHarvester(shortName, conf, outputDir, logger) {

  override protected val mimeType: String = "application_xml"

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
    * Takes care of parsing an xml file into a list of Nodes each representing an item
    *
    * @param xml Root of the xml document
    * @return List of Options of id/item pairs.
    */
  def handleXML(xml: Node): List[Option[ParsedResult]] = {
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
        Some(ParsedResult(id, outputXML))
      case _ =>
        logger.warn("Got weird result back for item path: " + item.getClass)
        None
    }
  }

  /**
    * Main logic for handling individual entries in the tar.
    *
    * @param tarResult  Case class representing extracted item from the tar
    * @return Count of metadata items found.
    */
  def handleFile(tarResult: FileResult,
                 unixEpoch: Long): Try[Int] =
    tarResult.data match {
      case None =>
        Success(0) //a directory, no results

      case Some(data) =>
        Try {
          val xml = XML.loadString(new String(data))
          val items = handleXML(xml)
          val entryName = tarResult.entryName
          // log the file name
          logger.info(entryName)

          val counts = for {
            itemOption <- items
            item <- itemOption // filters out the Nones
          } yield {
            writeOut(unixEpoch, item)
            1
          }
          counts.sum
        }
    }

  /**
    * Implements a stream of files from the tar.
    * Can't use @tailrec here because the compiler can't recognize it as tail recursive,
    * but this won't blow the stack.
    *
    * @param tarInputStream
    * @return Lazy stream of tar recordsd
    */
  def iter(tarInputStream: TarInputStream): Stream[FileResult] =
    Option(tarInputStream.getNextEntry) match {
      case None =>
        Stream.empty

      case Some(entry) =>
        val result =
          if (entry.isDirectory)
            None
          else
            Some(IOUtils.toByteArray(tarInputStream, entry.getSize))
        FileResult(entry.getName, result) #:: iter(tarInputStream)
    }


  /**
    * Executes the plains2peaks harvest
    */
  protected def localHarvest(): Unit = {
    val harvestTime = System.currentTimeMillis()
    val unixEpoch = harvestTime  / 1000L
    val inFile = new File(conf.harvest.endpoint.getOrElse("in"))

    if (inFile.isDirectory)
      for (file: File <- inFile.listFiles())
        harvestFile(file, unixEpoch)
    else
      harvestFile(inFile, unixEpoch)

  }

  private def harvestFile(file: File, unixEpoch: Long): Unit = {
    val inputStream = getInputStream(file)
      .getOrElse(throw new IllegalArgumentException("Couldn't load tar file."))

    val recordCount = (for (tarResult <- iter(inputStream)) yield {
      handleFile(tarResult, unixEpoch) match {
        case Failure(exception) =>
          logger.error(s"Caught exception on ${tarResult.entryName}.", exception)
          0
        case Success(count) =>
          count
      }
    }).sum

    IOUtils.closeQuietly(inputStream)
  }

  /**
    * Converts a Node to an xml string
    *
    * @param node The root of the tree to write to a string
    * @return a String containing xml
    */
  def xmlToString(node: Node): String =
    Utility.serialize(node, minimizeTags = MinimizeMode.Always).toString
}