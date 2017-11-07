package dpla.ingestion3

import java.net.URI
import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}

import com.databricks.spark.avro.SchemaConverters
import dpla.ingestion3.model.DplaMapData.LiteralOrUri
import dpla.ingestion3.utils.FlatFileIO
import org.apache.avro.Schema
import org.apache.spark.sql.types.StructType
import org.json4s.DefaultFormats
import org.json4s.JsonAST._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.prefs.EmptyValueStrategy


package object model {

  val DcmiTypes =
    Set("collection", "dataset", "event", "image", "interactiveresource", "service", "software", "sound", "text")

  def nameOnlyAgent(string: String): EdmAgent = EdmAgent(name = Some(string))

  def nameOnlyPlace(string: String): DplaPlace = DplaPlace(name = Some(string))

  def stringOnlyTimeSpan(string: String): EdmTimeSpan = EdmTimeSpan(originalSourceDate = Option(string))

  def nameOnlyConcept(string: String): SkosConcept = SkosConcept(providedLabel = Some(string))

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

  def jsonlRecord(record: OreAggregation): String = {
    // We require the that DPLA ID and prehash ID (the providers 'permanent' identifier) be passed forward via the
    // metadata sidecar. Currently, there is no place to store these values in either the DPLA MAPv3.1 or MAPv4.0 models
    val dplaId = record.sidecar \ "dplaId" match {
      case JString(js) => js
      case _ => throw new RuntimeException("DPLA ID is not in metadata sidecar")
    }
    val _id = record.sidecar \ "prehashId" match {
      case JString(js) => js
      case _ => throw new RuntimeException("Prehash ID is not in metadata sidecar")
    }

    val jobj: JObject =
      ("_type" -> "item") ~
      ("_id" -> _id) ~
      ("_source" ->
        ("id" -> dplaId) ~
        ("_id" -> _id) ~
        ("@context" -> "http://dp.la/api/items/context") ~
        ("@id" -> record.dplaUri.toString) ~
        ("aggregatedCHO" -> "#sourceResource") ~
        ("dataProvider" -> record.dataProvider.name) ~
        ("ingestDate" -> ingestDate) ~
        ("ingestType" -> "item") ~
        ("isShownAt" -> record.isShownAt.uri.toString) ~
        ("object" -> record.`preview`.map{o => o.uri.toString}) ~ // in dpla map 3, object is the thumbnail
        ("rights" -> record.edmRights.map(r => r.toString)) ~
        ("originalRecord" ->
          ("stringValue" -> record.originalRecord )) ~  // work around b/c original record viewer in CQA
                                                        // expects JSON and all ORs in ingest1 were converted to JSON
                                                        // TODO We need to prettify this so the OR is readable in CQA
        ("provider" ->
          ("@id" -> record.provider.uri
                      .getOrElse(
                        throw new RuntimeException("Invalid Provider URI")
                      ).toString) ~
          ("name" -> record.provider.name)) ~
        ("sourceResource" ->
          ("@id" ->
            (record.dplaUri.toString + "#SourceResource")) ~
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
              "name" -> lang.concept.getOrElse(
                          lang.providedLabel.getOrElse(""))
               // FIXME what other SkosConcept fields do we need in the index for language?
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
          ("subject" -> record.sourceResource.subject.map{s =>
            "name" -> s.providedLabel
          }) ~
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


    // Taken from
    // https://stackoverflow.com/questions/40128816/remove-json-field-when-empty-value-in-serialize-with-json4s
    implicit val formats = DefaultFormats.withEmptyValueStrategy(new EmptyValueStrategy {
      def noneValReplacement = None

      def replaceEmpty(value: JValue): JValue = value match {
        case JString("") => JNothing
        case JArray(items) =>
          if(items.isEmpty) JNothing
          else JArray(items.map(replaceEmpty))
        case JObject(fields) => JObject(fields map {
          case JField(name, v) => JField(name, value = replaceEmpty(v))
        })
        case oth => oth
      }
    })

    compact(render(jobj)(formats))
  }

  val avroSchema: Schema = new Schema.Parser().parse(new FlatFileIO().readFileAsString("/avro/MAPRecord.avsc"))
  val sparkSchema: StructType = SchemaConverters.toSqlType(avroSchema).dataType.asInstanceOf[StructType]

}
