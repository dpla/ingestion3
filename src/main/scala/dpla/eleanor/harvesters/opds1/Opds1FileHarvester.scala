package dpla.eleanor.harvesters.opds1

import java.io.{File, FileInputStream, InputStreamReader}

import dpla.eleanor.Schemata.{HarvestData, Payload}
import dpla.eleanor.harvesters.Retry
import dpla.eleanor.{HarvestStatics, Schemata}
import org.apache.commons.io.IOUtils

import scala.xml._

/**
  * Responsible for harvesting a XML file into a Seq[HarvestData]
  *
  */
trait Opds1FileHarvester extends Retry {

  case class Link(href: String, rel: String, title: String, `type`: String)

  def getId(xml: Elem): String = Option((xml \ "id").text.trim).getOrElse(throw new RuntimeException("Missing required ID"))

  def harvestFile(file: String, statics: HarvestStatics): Seq[HarvestData] = {
    val inputStream = new InputStreamReader(new FileInputStream(new File(file)))

    // create lineIterator to read contents one line at a time
    val iter = IOUtils.lineIterator(inputStream)

    var rows: Seq[HarvestData] = Seq[HarvestData]()

    while (iter.hasNext) {
      Option(iter.nextLine) match {
        case Some(line) => handleLine(line, statics) match {
          case Some(t) => rows = rows ++: Seq(t)
          case None => rows
        }
        case _ => // do nothing
      }
    }
    IOUtils.closeQuietly(inputStream)

    rows
  }

  def handleLine(line: String, statics: HarvestStatics): Option[HarvestData] =
    Option(line) match {
      case None => None
      case Some(data) =>
        val dataString = new String(data).replaceAll("<\\?xml.*\\?>", "")
        val xml = XML.loadString(dataString)
        val bytes = xmlToBytes(xml)
        val id = getId(xml)

        Option(HarvestData(
          sourceUri = statics.sourceUri,
          timestamp = statics.timestamp,
          id = id,
          metadataType = statics.metadataType,
          metadata = bytes,
          payloads = getPayloads(xml)
        ))
      }

  def getPayloads(record: Node): Seq[Schemata.Payload] = {
    val links = for (link <- record \ "link")
      yield Link(link \@ "href", link \@ "rel", link \@ "title", link \@ "type")
    links.collect {
      // collects *all* top level <link> properties, can be more restrictive if required
      case Link(url, _, _, _) => Payload(url = s"$url") // FIXME confirm links
    }
  }

  def xmlToBytes(node: Node): Array[Byte] =
    Utility.serialize(node, minimizeTags = MinimizeMode.Always).toString.getBytes
}
