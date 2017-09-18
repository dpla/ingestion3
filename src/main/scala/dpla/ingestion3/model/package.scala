package dpla.ingestion3

import java.net.URI

import com.databricks.spark.avro.SchemaConverters
import dpla.ingestion3.model.DplaMapData.LiteralOrUri
import dpla.ingestion3.utils.FlatFileIO
import org.json4s.JsonAST.JObject
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import dpla.ingestion3.utils.Utils.generateMd5
import org.apache.avro.Schema
import org.apache.spark.sql.types.StructType
import java.util.{Calendar, TimeZone}
import java.text.SimpleDateFormat


package object model {

  val DcmiTypes =
    Set("collection", "dataset", "event", "image", "interactiveresource", "service", "software", "sound", "text")

  def nameOnlyAgent(string: String): EdmAgent = EdmAgent(name = Some(string))

  def nameOnlyPlace(string: String): DplaPlace = DplaPlace(name = Some(string))

  def stringOnlyTimeSpan(string: String): EdmTimeSpan = EdmTimeSpan(originalSourceDate = Option(string))

  def nameOnlyConcept(string: String): SkosConcept = SkosConcept(concept = Some(string))

  def nameOnlyCollection(string: String): DcmiTypeCollection = DcmiTypeCollection(title = Some(string))

  def uriOnlyWebResource(uri: URI): EdmWebResource = EdmWebResource(uri = uri)

  def isDcmiType(string: String): Boolean = DcmiTypes.contains(string.toLowerCase.replaceAll(" ", ""))

  def eitherStringOrUri(string: String): LiteralOrUri = new Left(string)

  def eitherStringOrUri(uri: URI): LiteralOrUri = new Right(uri)

  lazy val ingestDate: String = {
    val now = Calendar.getInstance().getTime
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    val tz = TimeZone.getTimeZone("UTC")
    sdf.setTimeZone(tz)
    sdf.format(now)
  }

  lazy val providerToken: (Option[URI]) => String = {
    (uri: Option[URI]) => {
      uri match {
        case Some(x) =>
          val pat = """.*/([a-z]+)$""".r
          val pat(token) = x.toString
          token
        case _ =>
          throw new RuntimeException("Invalid provider URI")
      }
    }
  }

  def jsonlRecord(record: DplaMapData): String = {
    val recordID: String = generateMd5(Some(record.oreAggregation.uri.toString))
    val jobj: JObject =
      ("_type" -> "item") ~
      // For _id, we should have a provider token like "nara" or "ia"
      ("_id" ->
        (s"${providerToken(record.oreAggregation.provider.uri)}--" +
         s"${record.oreAggregation.uri.toString}")) ~
      ("_source" ->
        ("id" -> recordID) ~
        ("_id" ->
          (s"${providerToken(record.oreAggregation.provider.uri)}--" +
           s"${record.oreAggregation.uri.toString}")) ~
        ("@context" -> "http://dp.la/api/items/context") ~
        ("@id" -> ("http://dp.la/api/items/" + recordID)) ~
        ("admin" ->
          ("sourceResource" -> ("title" -> record.sourceResource.title))) ~
        ("aggregatedCHO" -> "#sourceResource") ~
        ("dataProvider" -> record.oreAggregation.dataProvider.name) ~
        ("ingestDate" -> ingestDate) ~
        ("ingestType" -> "item") ~
        ("isShownAt" -> record.edmWebResource.uri.toString) ~
        ("object" -> record.oreAggregation.`object`.map{o => o.uri.toString}) ~
        ("originalRecord" ->
          ("stringValue" -> record.oreAggregation.originalRecord )) ~
        ("provider" ->
          ("@id" -> record.oreAggregation.provider.uri
                      .getOrElse(
                        throw new RuntimeException("Invalid Provider URI")
                      ).toString) ~
          ("name" -> record.oreAggregation.provider.name)) ~
        ("sourceResource" ->
          ("@id" ->
            ("http://dp.la/api/items/" + recordID + "#SourceResource")) ~
          ("collection" ->
            record.sourceResource.collection.map {c => "title" -> c.title}) ~
          ("contributor" -> record.sourceResource.contributor.map{c => c.name}) ~
          ("creator" -> record.sourceResource.creator.map{c => c.name}) ~
          ("date" ->
            record.sourceResource.date.map { d =>
              ("displayDate" -> d.originalSourceDate) ~
              ("begin" -> d.begin) ~
              ("end" -> d.end)
            }) ~
          ("description" -> record.sourceResource.description) ~
          ("extent" -> record.sourceResource.extent) ~
          ("format" -> record.sourceResource.format) ~
          ("identifier" -> record.sourceResource.identifier) ~
          ("language" ->
            record.sourceResource.language.map { lang =>
              ("name" -> lang.providedLabel) ~ ("iso639_3" -> lang.concept)
            }) ~
          ("publisher" -> record.sourceResource.publisher.map{p => p.name}) ~
          ("relation" ->
            record.sourceResource.relation.map { r =>
              r.merge.toString
            })  ~
          ("rights" -> record.sourceResource.rights) ~
          ("spatial" ->
            record.sourceResource.place.map { place =>
              ("name" -> place.name) ~
              ("city" -> place.city) ~
              ("county" -> place.county) ~
              ("state" -> place.state) ~
              ("country" -> place.country) ~
              ("coordinates" -> place.coordinates)
            }) ~
          /*
           * FIXME: specType is unaccounted for in DplaMapData.
           * It was edm:hasType in MAPv3.1, and was optional.
           */
          // ("specType" -> "FIXME") ~
          // stateLocatedIn is omitted.
          ("subject" -> record.sourceResource.subject.map{s => s.providedLabel}) ~
          ("temporal" ->
            record.sourceResource.temporal.map{ t =>
              ("displayDate" -> t.originalSourceDate) ~
              ("begin" -> t.begin) ~
              ("end" -> t.end)
            }) ~
          ("title" -> record.sourceResource.title) ~
          ("type" -> record.sourceResource.`type`)
        ) ~
        ("@type" -> "ore:Aggregation")
      )


    compact(render(jobj))
  }

  val avroSchema: Schema = new Schema.Parser().parse(new FlatFileIO().readFileAsString("/avro/MAPRecord.avsc"))
  val sparkSchema: StructType = SchemaConverters.toSqlType(avroSchema).dataType.asInstanceOf[StructType]

}
