package dpla.eleanor.harvesters.providers

import java.io._
import java.sql.Timestamp
import java.util.zip.ZipInputStream

import dpla.eleanor.Schemata.{HarvestData, MetadataType, Payload, SourceUri}
import dpla.eleanor.harvesters.ContentHarvester
import dpla.eleanor.{HarvestStatics, Schemata}
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import scala.xml._

class GutenbergHarvester(timestamp: Timestamp, source: SourceUri, metadataType: MetadataType)
  extends ContentHarvester with Serializable {
  /**
    *
    * @param spark            Spark session
    * @param out              Could he local or s3
    * @return
    */
  def execute(spark: SparkSession,
              files: Seq[String] = Seq(),
              out: String): Dataset[HarvestData] = {

    import spark.implicits._

    val harvestStatics: HarvestStatics = HarvestStatics(
      sourceUri = source.uri,
      timestamp = timestamp,
      metadataType = metadataType
    )

    // Harvest file into HarvestData
    val rows: Seq[HarvestData] = files.flatMap(xmlFile => harvestFile(xmlFile, harvestStatics))
    val ds = spark.createDataset(rows)

    println("Harvested file")

    // Run over HarvestData Dataset and download content/payloads
    val contentDs = harvestContent(ds, spark)

    // Write out complete HarvestData Dataset
    val dataOut = out + "data.parquet/" // fixme hardcoded output
    println(s"Writing to $dataOut")
    contentDs.write.mode(SaveMode.Append).parquet(dataOut)

    contentDs
  }

  def harvestFile(file: String, statics: HarvestStatics): Seq[HarvestData] = {
    var rows: Seq[HarvestData] = Seq[HarvestData]()

    val inputStream = getInputStream(new File(file))
      .getOrElse(throw new IllegalArgumentException("Couldn't load ZIP files."))

    val count = (for (result <- iter(inputStream)) yield {
      result.data match {
        case Some(xmlBytes) => {
          handleLine(xmlBytes, statics) match {
            case None => 0
            case Some(t) =>
              rows = rows ++: Seq(t)
              1
          }
        }
        case None => 0
      }
    }).sum

    IOUtils.closeQuietly(inputStream)

    rows
  }

  def handleLine(data: Array[Byte], statics: HarvestStatics): Option[HarvestData] = {
    val dataString = new String(data).replaceAll("<\\?xml.*\\?>", "")
    val xml = XML.loadString(dataString)
    val id = getId(xml)

    Option(HarvestData(
      sourceUri = statics.sourceUri,
      timestamp = statics.timestamp,
      id = id,
      metadataType = statics.metadataType,
      metadata = data,
      payloads = getPayloads(xml)
    ))
  }


  def getPayloads(record: Node): Seq[Schemata.Payload] = {
    val links = for (link <- record \ "hasFormat")
      yield Link(link \ "file" \@ "about", "TBD rel", "TBD title", (link \\ "value").text )
    links.collect {
      // collects *all* top level <link> properties, can be more restrictive if required
      case Link(url, _, _, _) => Payload(url = s"$url")
    }
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
          if (entry.isDirectory || !entry.getName.endsWith(".rdf"))
            None
          else {
            Some(IOUtils.toByteArray(zipInputStream, entry.getSize))
          }
        FileResult(entry.getName, result) #:: iter(zipInputStream)
    }

  case class Link(href: String, rel: String, title: String, `type`: String)

  // ebook \@ rdf:about
  def getId(xml: Elem): String =
    Option((xml \ "ebook" \@ s"{${xml.getNamespace("rdf")}}about").trim)
      .getOrElse(throw new RuntimeException("Missing required ID"))
}


/**
  * Case class to hold the results of a file
  *
  * @param entryName    Path of the entry in the file
  * @param data         Holds the data for the entry, or None if it's a directory.
  * @param bufferedData Holds a buffered reader for the entry if it's too
  *                     large to be held in memory.
  */
case class FileResult(entryName: String,
                      data: Option[Array[Byte]],
                      bufferedData: Option[BufferedReader] = None)

