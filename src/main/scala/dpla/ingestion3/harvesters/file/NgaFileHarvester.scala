package dpla.ingestion3.harvesters.file

import com.opencsv.CSVReader
import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.harvesters.{LocalHarvester, ParsedResult}
import dpla.ingestion3.model.AVRO_MIME_JSON
import org.apache.avro.generic.GenericData
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._

import java.io.{File, FileReader}
import scala.collection.mutable
import scala.util.Using

/** TEST HUB — NOT APPROVED FOR PRODUCTION
  *
  * File harvester for the National Gallery of Art Open Data Program
  * ([[https://github.com/NationalGalleryOfArt/opendata]]), a set of CC0 CSV
  * files exported from NGA's TMS collection-management system. Modeled on the
  * Digital Virginias GitHub-sourced flow: an acquisition step clones the repo to
  * a local directory (or S3 prefix), then this `harvest.type = "file"` harvester
  * reads the CSVs from `harvest.endpoint`.
  *
  * NGA data is relational — the descriptive record for one art object is spread
  * across several CSVs joined by `objectid`. Because a DPLA `OriginalRecord` must
  * be self-contained (the mapper does not perform cross-file joins), this
  * harvester assembles one JSON document per object by joining:
  *
  *   - objects.csv               core object row (one per objectid)
  *   - published_images.csv      images, joined via depictstmsobjectid
  *   - objects_constituents.csv  object↔constituent links (artist/donor/owner)
  *   - constituents.csv          constituent names + ULAN / Wikidata IDs
  *   - objects_terms.csv         keywords / place / style / technique / theme
  *   - objects_text_entries.csv  narrative, bibliography, exhibition history, …
  *
  * Each object's assembled document carries the object's own columns at the top
  * level plus nested `images`, `constituents`, `terms`, and `textEntries`
  * arrays. Constituent links are enriched with the looked-up constituent record
  * (forwardDisplayName, ulanid, wikidataid, …) so the mapper has names and
  * authority IDs without a second join.
  */
class NgaFileHarvester(
    spark: SparkSession,
    shortName: String,
    conf: i3Conf
) extends LocalHarvester(shortName, conf) {

  private val logger = LogManager.getLogger(this.getClass)

  override def mimeType: GenericData.EnumSymbol = AVRO_MIME_JSON

  override def harvest: DataFrame = {
    val harvestTime = System.currentTimeMillis()
    val unixEpoch = harvestTime / 1000L
    val endpoint = conf.harvest.endpoint.getOrElse(
      throw new RuntimeException("nga.harvest.endpoint is not set")
    )
    // Accepts a local directory or an s3:// prefix (synced to a temp dir).
    val dataDir = LocalHarvester
      .resolveToLocalDir(endpoint, harvestTime, "nga-s3", conf.harvest.awsProfile)
    require(dataDir.isDirectory, s"NGA data directory not found: $dataDir")

    // Child tables are grouped by their join key while streaming — the whole
    // file is never held as an intermediate list. objects.csv is likewise
    // streamed row by row, so no single table is fully materialized.
    def in(name: String) = new File(dataDir, name)
    val imagesByObj = NgaFileHarvester.indexBy(in("published_images.csv"), "depictstmsobjectid")
    val constLinksByObj = NgaFileHarvester.indexBy(in("objects_constituents.csv"), "objectid")
    val constById = NgaFileHarvester.indexUniqueBy(in("constituents.csv"), "constituentid")
    val termsByObj = NgaFileHarvester.indexBy(in("objects_terms.csv"), "objectid")
    val textsByObj = NgaFileHarvester.indexBy(in("objects_text_entries.csv"), "objectid")

    var count = 0
    NgaFileHarvester.foreachRow(in("objects.csv")) { obj =>
      obj.get("objectid").filter(_.nonEmpty) match {
        case None =>
          logger.warn(s"Skipping objects.csv row with no objectid: $obj")
        case Some(objectId) =>
          val doc = NgaFileHarvester.buildDocument(
            obj,
            imagesByObj,
            constLinksByObj,
            constById,
            termsByObj,
            textsByObj
          )
          writeOut(unixEpoch, ParsedResult(objectId, compact(render(doc))))
          count += 1
      }
    }
    logger.info(s"Harvested $count NGA objects")

    close()
    spark.read.format("avro").load(tmpOutStr)
  }
}

object NgaFileHarvester {

  /** Streams a CSV row by row, applying `f` to each column→value map. opencsv
    * handles quoted fields with embedded commas and newlines (common in NGA's
    * medium / dimensions / bibliography columns). Each row is zipped against the
    * header rather than using the strict header-aware `readMap()`: NGA's export
    * contains occasional ragged rows (a field short/long of the header count),
    * and zipping tolerates them (missing trailing columns are simply absent)
    * instead of aborting the entire harvest on one bad row.
    */
  def foreachRow(file: File)(f: Map[String, String] => Unit): Unit =
    Using.resource(new CSVReader(new FileReader(file))) { reader =>
      Option(reader.readNext()).foreach { header =>
        Iterator
          .continually(reader.readNext())
          .takeWhile(_ != null)
          .foreach(cols => f(header.zip(cols).toMap))
      }
    }

  /** Reads a CSV with a header row into a list of column→value maps. */
  def readRows(file: File): List[Map[String, String]] = {
    val buf = List.newBuilder[Map[String, String]]
    foreachRow(file)(buf += _)
    buf.result()
  }

  /** Streams `file` and groups rows by the value of `key`, preserving row order
    * within each group. Rows whose key is missing or empty are dropped. Rows are
    * grouped directly into the result, so the file is never held as an
    * intermediate list.
    */
  def indexBy(file: File, key: String): Map[String, List[Map[String, String]]] = {
    val groups = mutable.LinkedHashMap.empty[String, mutable.ListBuffer[Map[String, String]]]
    foreachRow(file) { row =>
      row.get(key).filter(_.nonEmpty).foreach { k =>
        groups.getOrElseUpdate(k, mutable.ListBuffer.empty) += row
      }
    }
    groups.view.mapValues(_.toList).toMap
  }

  /** Streams `file` into a map keyed by a unique `key` column (last row wins on a
    * duplicate key). Used for the constituents lookup table.
    */
  def indexUniqueBy(file: File, key: String): Map[String, Map[String, String]] = {
    val map = mutable.LinkedHashMap.empty[String, Map[String, String]]
    foreachRow(file) { row =>
      row.get(key).filter(_.nonEmpty).foreach(k => map(k) = row)
    }
    map.toMap
  }

  private def rowToJObject(row: Map[String, String]): JObject =
    JObject(row.toList.map { case (k, v) => k -> JString(v) })

  /** Assembles the self-contained JSON document for one art object by joining
    * its child rows. Pure function of its inputs — the unit test exercises the
    * join logic here directly.
    */
  def buildDocument(
      obj: Map[String, String],
      imagesByObj: Map[String, List[Map[String, String]]],
      constLinksByObj: Map[String, List[Map[String, String]]],
      constById: Map[String, Map[String, String]],
      termsByObj: Map[String, List[Map[String, String]]],
      textsByObj: Map[String, List[Map[String, String]]]
  ): JValue = {
    val objectId = obj.getOrElse("objectid", "")

    val images = imagesByObj.getOrElse(objectId, Nil).map(rowToJObject)

    // Each object↔constituent link, enriched with the looked-up constituent
    // record under "constituent" so the mapper gets the name + ULAN/Wikidata IDs
    // without a second join.
    val constituents = constLinksByObj.getOrElse(objectId, Nil).map { link =>
      link.get("constituentid").flatMap(constById.get) match {
        case Some(c) => rowToJObject(link) ~ ("constituent" -> rowToJObject(c))
        case None    => rowToJObject(link)
      }
    }

    val terms = termsByObj.getOrElse(objectId, Nil).map(rowToJObject)
    val texts = textsByObj.getOrElse(objectId, Nil).map(rowToJObject)

    rowToJObject(obj) ~
      ("images" -> JArray(images)) ~
      ("constituents" -> JArray(constituents)) ~
      ("terms" -> JArray(terms)) ~
      ("textEntries" -> JArray(texts))
  }
}
