package dpla.ingestion3.model

import dpla.ingestion3.utils.FlatFileIO
import org.json4s._
import org.json4s.jackson.JsonMethods._

/** Maps DPLA item IDs (32-hex) to the slugs of the exhibitions and primary
  * source sets they appear in. `generated` is the snapshot crawl timestamp.
  */
case class CuratedMembership(
    generated: String,
    exhibitions: Map[String, Seq[String]],
    primarySourceSets: Map[String, Seq[String]]
)

object CuratedMembership {

  /** Written by scripts/curated_membership.py.
    * Re-run it and rebuild the JAR when curated content changes.
    */
  val resourceName = "/curated/curated-membership.json"

  val empty: CuratedMembership = CuratedMembership("", Map.empty, Map.empty)

  lazy val fromResource: CuratedMembership = {
    val json = parse(new FlatFileIO().readFileAsString(resourceName))
    val items = json \ "items" match {
      case JObject(fields) => fields
      case _ =>
        throw new RuntimeException(
          s"$resourceName is missing the items object"
        )
    }
    def slugs(value: JValue): Seq[String] = value match {
      case JArray(values) => values.collect { case JString(s) => s }
      case _              => Seq.empty
    }
    def index(field: String): Map[String, Seq[String]] =
      items.flatMap { case (id, value) =>
        val s = slugs(value \ field)
        if (s.nonEmpty) Some(id -> s) else None
      }.toMap
    val generated = json \ "generated" match {
      case JString(s) => s
      case _ =>
        throw new RuntimeException(
          s"$resourceName is missing the generated timestamp"
        )
    }
    CuratedMembership(generated, index("exhibitions"), index("primarySourceSets"))
  }
}
