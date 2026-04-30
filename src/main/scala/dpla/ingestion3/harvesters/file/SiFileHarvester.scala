package dpla.ingestion3.harvesters.file

import java.io.{File, FileInputStream}
import java.util.zip.GZIPInputStream
import javax.xml.stream.{XMLInputFactory, XMLStreamConstants}
import org.apache.hadoop.shaded.com.ctc.wstx.api.WstxInputProperties   
import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.harvesters.{Harvester, LocalHarvester, ParsedResult}
import dpla.ingestion3.harvesters.file.FileFilters.gzFilter
import dpla.ingestion3.model.AVRO_MIME_XML
import dpla.ingestion3.utils.Utils
import org.apache.avro.generic.GenericData
import org.apache.commons.io.FileUtils
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.Source
import scala.util.{Try, Using}
import scala.xml.{Node, Utility, XML}

/** Entry for performing Smithsonian file harvest
  */
class SiFileHarvester(
    spark: SparkSession,
    shortName: String,
    conf: i3Conf
) extends LocalHarvester(shortName, conf) {

  private val logger = LogManager.getLogger(this.getClass)

  def mimeType: GenericData.EnumSymbol = AVRO_MIME_XML

  /** Takes care of parsing an xml file into a list of Nodes each representing
    * an item
    *
    * @param xml
    *   Root of the xml document
    * @return
    *   List of Options of id/item pairs.
    */
  def handleXML(xml: Node): List[Option[ParsedResult]] =
    for {
      items <- xml \\ "doc" :: Nil
      item <- items
    } yield item match {
      case record: Node =>
        // Extract required record identifier
        val id = Option(
          (record \ "descriptiveNonRepeating" \ "record_ID").text
        )
        val outputXML = Harvester.xmlToString(record)

        id match {
          case None =>
            logger.warn(s"Missing required record_ID for $outputXML")
            None
          case Some(id) => Some(ParsedResult(id, outputXML))
        }
      case _ =>
        logger.warn("Got weird result back for item path: " + item.getClass)
        None
    }

  /** Get the expected record counts for each provided file from a text file
    * provided along side metadata. Contents of file are expected to match this
    * format:
    *
    * AAADCD_DPLA.xml records = 15,234 ACAH_DPLA.xml records = 0 ACM_DPLA.xml
    * records = 1,500 CHNDM_DPLA.xml records = 156,226
    *
    * @param inFiles
    *   File Input directory with metadata and summary file
    * @return
    *   Map[String, String] Map of filename to record count (formatted as string
    *   in file)
    */
  private def getExpectedFileCounts(inFiles: File): Map[String, String] = {
    var loadCounts = Map[String, String]()
    Option(inFiles.listFiles(FileFilters.txtFilter))
      .getOrElse(Array.empty)
      .foreach(file => {
        Using(
          Source
            .fromFile(file)
        ) { source =>
          source
            .getLines()
            .foreach(line => {
              Try {
                val lineVals = line.split(" records = ")
                // rename .xml to .xml.gz to match filename processed by harvester
                lineVals(0).replace(".xml", ".xml.gz") -> lineVals(1)
              } match {
                case scala.util.Success(row) => loadCounts += row
                case scala.util.Failure(_)   => loadCounts
              }
            })
        }
      })
    loadCounts
  }

  private def resolveToLocalDir(endpoint: String, harvestTime: Long): File = {
    if (endpoint.startsWith("s3://"))
      logger.info(s"Syncing SI source from S3: $endpoint")
    LocalHarvester.resolveToLocalDir(endpoint, harvestTime, "si-s3", conf.harvest.awsProfile)
  }

  /** Collects a complete &lt;doc&gt;…&lt;/doc&gt; element from a streaming XML
    * reader as a String. The reader must be positioned at the START_ELEMENT
    * event for "doc".
    */
  private def collectDocXml(reader: javax.xml.stream.XMLStreamReader): String = {
    val sb = new StringBuilder("<doc")
    for (i <- 0 until reader.getAttributeCount) {
      sb.append(s""" ${reader.getAttributeLocalName(i)}="${Utility.escape(reader.getAttributeValue(i))}"""")
    }
    sb.append(">")

    var depth = 1
    while (depth > 0) {
      reader.next()
      reader.getEventType match {
        case XMLStreamConstants.START_ELEMENT =>
          depth += 1
          sb.append(s"<${reader.getLocalName}")
          for (i <- 0 until reader.getAttributeCount) {
            sb.append(
              s""" ${reader.getAttributeLocalName(i)}="${Utility.escape(reader.getAttributeValue(i))}""""
            )
          }
          sb.append(">")
        case XMLStreamConstants.END_ELEMENT =>
          depth -= 1
          if (depth > 0) sb.append(s"</${reader.getLocalName}>")
        case XMLStreamConstants.CHARACTERS | XMLStreamConstants.SPACE =>
          sb.append(Utility.escape(reader.getText))
        case XMLStreamConstants.CDATA =>
          sb.append(s"<![CDATA[${reader.getText}]]>")
        case _ => // skip comments, PIs, etc.
      }
    }
    sb.append("</doc>")
    sb.toString()
  }

  /** Executes the Smithsonian harvest
    */
  override def harvest: DataFrame = {
    val harvestTime = System.currentTimeMillis()
    val unixEpoch = harvestTime / 1000L
    val endpoint = conf.harvest.endpoint.getOrElse("in")
    val isTempDir = endpoint.startsWith("s3://")
    val inFiles = resolveToLocalDir(endpoint, harvestTime)

    val expectedFileCounts = getExpectedFileCounts(inFiles)

    // SI XML uses no namespace prefixes on elements or attributes inside <doc>,
    // so IS_NAMESPACE_AWARE=false is safe and avoids the overhead of namespace processing.
    // DTD and external entity resolution are disabled to prevent XXE attacks.
    val xmlInputFactory = XMLInputFactory.newInstance()
        xmlInputFactory.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, false)
        xmlInputFactory.setProperty(XMLInputFactory.IS_COALESCING, true)
        xmlInputFactory.setProperty(XMLInputFactory.SUPPORT_DTD, false)
        xmlInputFactory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false)
        xmlInputFactory.setProperty(                                                  
            WstxInputProperties.P_INPUT_PARSING_MODE,                                   
            WstxInputProperties.PARSING_MODE_DOCUMENTS                                  
            )                                                                               

    try {
      Option(inFiles.listFiles(gzFilter)).getOrElse(Array.empty).foreach { inFile =>
        var lineCount = 0

        Using(new GZIPInputStream(new FileInputStream(inFile))) { gzStream =>
          val reader = xmlInputFactory.createXMLStreamReader(gzStream)
          try {
            while (reader.hasNext) {
              reader.next()
              if (
                reader.getEventType == XMLStreamConstants.START_ELEMENT &&
                reader.getLocalName == "doc"
              ) {
                Try(XML.loadString(collectDocXml(reader))) match {
                  case scala.util.Success(node) =>
                    handleXML(node).foreach {
                      case Some(item) => writeOut(unixEpoch, item); lineCount += 1
                      case None       =>
                    }
                  case scala.util.Failure(e) =>
                    logger.error(s"Failed to parse <doc> in ${inFile.getName}", e)
                }
              }
            }
          } finally {
            reader.close()
          }
        }.recover { case e => logger.error(s"Failed to process ${inFile.getName}", e) }

        // Log a summary of the file. expectedFileCounts keys match the full filename (e.g. "AAADCD_DPLA.xml.gz").
        val fromFilePretty = Utils.formatNumber(lineCount.toLong)
        val expectedPretty = expectedFileCounts.getOrElse(inFile.getName, "0")
        val expectedFloat =
          Try(expectedPretty.replaceAll(",", "").trim.toFloat).getOrElse(0f)
        val percentage = (lineCount.toFloat / expectedFloat) * 100.0f match {
          case x if x.isNaN      => 0.0f
          case x if x.isInfinity => lineCount.toFloat
          case x                 => x.intValue()
        }

        logger.info(
          s"Harvested $percentage% ($fromFilePretty / $expectedPretty) of records from ${inFile.getName}"
        )
      }
    } finally {
      if (isTempDir) FileUtils.deleteQuietly(inFiles)
    }

    close()

    // Read harvested data into Spark DataFrame and return.
    spark.read.format("avro").load(tmpOutStr)
  }

}
