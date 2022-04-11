package dpla.eleanor.harvesters.providers

import java.io._
import java.sql.Timestamp
import java.util.zip.ZipInputStream

import dpla.eleanor.Schemata.{HarvestData, MetadataType, Payload, SourceUri}
import dpla.eleanor.harvesters.ContentHarvester
import dpla.eleanor.{HarvestStatics, Schemata}
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.xml._

class GpoHarvester(timestamp: Timestamp, source: SourceUri, metadataType: MetadataType)
  extends ContentHarvester with Serializable {
  /**
    *
    * @param spark            Spark session
    * @param out              Could he local or s3
    * @return
    */
  def execute(spark: SparkSession,
              files: Seq[String] = Seq(),
              out: String) = {

    import spark.implicits._

    val harvestStatics: HarvestStatics = HarvestStatics(
      sourceUri = source.uri,
      timestamp = timestamp,
      metadataType = metadataType
    )

    val dataOut = out + "test_data.parquet/" // fixme hardcoded output
    // Harvest file into HarvestData
    files.foreach(xmlFile =>{
      val rows = harvestFile(xmlFile, harvestStatics)
      val ds = spark.createDataset(rows)
      println(s"Harvested file $xmlFile")
      println(s"Writing to $dataOut")
      ds.write.mode(SaveMode.Append).parquet(dataOut)
    })
  }

  def harvestFile(file: String, statics: HarvestStatics): Seq[HarvestData] = {
    println(s"Harvesting ..$file")
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

  def handleLine(data: String, statics: HarvestStatics): Option[HarvestData] = {
    val dataString = data.replaceAll("<\\?xml.*\\?>", "")
    val xml = XML.loadString(dataString)
    val id = getId(xml)

    Option(HarvestData(
      sourceUri = statics.sourceUri,
      timestamp = statics.timestamp,
      id = id,
      metadataType = statics.metadataType,
      metadata = data.getBytes,
      payloads = getPayloads(xml)
    ))
  }

//   <marc:datafield ind2="0" ind1="4" tag="856">
//    <marc:subfield code="u">http://nces.ed.gov/nationsreportcard/pdf/main2005/2005463.pdf</marc:subfield>
//   </marc:datafield>

  def getPayloads(record: Node): Seq[Schemata.Payload] = {
    marcFields(record, Seq("856"), Seq("u")).map(url => {
      println(url.text)
      Payload(url = url.text)
    })
  }

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
    * @return Lazy stream of zip records
    */
  def iter(zipInputStream: ZipInputStream): Stream[FileResult] =
    Option(zipInputStream.getNextEntry) match {
      case None =>
        Stream.empty
      case Some(entry) =>
        val result =
          if (entry.isDirectory || !entry.getName.endsWith(".xml"))
            None
          else {
            Some(IOUtils.toByteArray(zipInputStream, entry.getSize))
          }
        FileResult(entry.getName, result) #:: iter(zipInputStream)
    }

  case class Link(href: String, rel: String, title: String, `type`: String)

  // header \ identifier
  def getId(xml: Elem): String =
    Option((xml \\ "header" \ "identifier").text.trim)
      .getOrElse(throw new RuntimeException("Missing required ID"))


  /**
    * Get <dataset><subfield> nodes by tag and code
    *
    * @param data   Document
    * @param tags   Seq[String] tags for <dataset>
    * @param codes  Seq[String] codes for <subfield> (if empty or undefined, all <subfield> nodes will be returned)
    * @return       Seq[NodeSeq] <subfield> nodes
    */
  def marcFields(data: NodeSeq, tags: Seq[String], codes: Seq[String] = Seq()): Seq[NodeSeq] = {
    val sub: Seq[NodeSeq] = datafield(data, tags).map(n => n \ "subfield")
    if (codes.nonEmpty) sub.map(n => filterSubfields(n, codes)) else sub
  }

  /**
    * Filter <subfield> nodes by code
    *
    * @param subfields  NodeSeq <subfield> nodes
    * @param codes      Seq[String] codes for <subfield>
    * @return           NodeSeq <subfield> nodes
    */
  def filterSubfields(subfields: NodeSeq, codes: Seq[String]): NodeSeq =
    subfields.flatMap(n => getByAttributeListOptions(n, "code", codes))

  /**
    * Get <dataset> nodes by tag
    *
    * @param data   Document
    * @param tags   Seq[String] tags for <dataset>
    * @return       NodeSeq <dataset> nodes
    */
  def datafield(data: NodeSeq, tags: Seq[String]): NodeSeq =
    (data \\ "datafield").flatMap(n => getByAttributeListOptions(n, "tag", tags))

  /**
    * Get nodes that match any of the given values for a given attribute.
    *
    * E.g. getByAttributesListOptions(elem, "color", Seq("red", "blue")) will return all nodes where
    *   attribute "color=red" or "color=blue"
    *
    * @param e Elem to examine
    * @param att String name of attribute
    * @param values Seq[String] Values of attribute
    * @return NodeSeq of matching nodes
    */
  def getByAttributeListOptions(e: Elem, att: String, values: Seq[String]): NodeSeq = {
    e \\ "_" filter { n => filterAttributeListOptions(n, att, values) }
  }

  def getByAttributeListOptions(e: NodeSeq, att: String, values: Seq[String]): NodeSeq = {
    getByAttributeListOptions(e.asInstanceOf[Elem], att, values)
  }

  /**
    * Exclude nodes that do not have an attribute that matches att and any of the given value parameters
    *
    * @param node Node XML node to examine
    * @param att String name of attribute
    * @param values Seq[String] all values of attributes
    * @return Boolean
    */
  def filterAttributeListOptions(node: Node, att: String, values: Seq[String]): Boolean =
    values.map(_.toLowerCase).contains((node \ ("@" + att)).text.toLowerCase)

}
