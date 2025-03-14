package dpla.ingestion3

import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}
import dpla.ingestion3.utils.FlatFileIO
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.avro.SchemaConverters
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
import org.json4s._

import org.json4s.prefs.EmptyValueStrategy
import org.json4s.{DefaultFormats, Formats}

package object model {

  private val DcmiTypes = Set(
    "collection",
    "dataset",
    "event",
    "image",
    "interactiveresource",
    "movingimage",
    "physicalobject",
    "service",
    "software",
    "sound",
    "stillimage",
    "text"
  )

  private val reservedWikiChars: List[String] = List(
    "|",
    "=",
    "[[",
    "]]",
    "{{",
    "}}",
    "''"
  )

  def nameOnlyAgent(string: String): EdmAgent = EdmAgent(name = Some(string))

  def nameOnlyPlace(string: String): DplaPlace = DplaPlace(name = Some(string))

  def stringOnlyTimeSpan(string: String): EdmTimeSpan =
    EdmTimeSpan(originalSourceDate = Option(string))

  def nameOnlyConcept(string: String): SkosConcept =
    SkosConcept(providedLabel = Some(string))

  def nameOnlyCollection(string: String): DcmiTypeCollection =
    DcmiTypeCollection(title = Some(string))

  def uriOnlyWebResource(uri: URI): EdmWebResource = EdmWebResource(uri = uri)

  def stringOnlyWebResource(uri: String): EdmWebResource =
    EdmWebResource(uri = URI(uri.trim))

  def isDcmiType(string: String): Boolean =
    DcmiTypes.contains(string.toLowerCase.replaceAll(" ", ""))

  def eitherStringOrUri(string: String): LiteralOrUri =
    LiteralOrUri(string, isUri = false)

  def eitherStringOrUri(uri: URI): LiteralOrUri =
    LiteralOrUri(uri.value, isUri = true)

  def emptyDplaPlace: DplaPlace = DplaPlace()

  def emptyEdmAgent: EdmAgent = EdmAgent()

  def emptyEdmTimeSpan: EdmTimeSpan = EdmTimeSpan()

  def emptyEdmWebResource: EdmWebResource = stringOnlyWebResource("")

  def emptyJValue: JValue = JNothing

  def emptyOreAggregation: OreAggregation = OreAggregation(
    dplaUri = URI(""),
    dataProvider = nameOnlyAgent(""),
    isShownAt = uriOnlyWebResource(URI("")),
    originalRecord = "",
    provider = nameOnlyAgent(""),
    sourceResource = DplaSourceResource(
      rights = Seq(""),
      title = Seq("")
    ),
    originalId = ""
  )

  lazy val ingestDate: String = {
    val now = Calendar.getInstance().getTime
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    val tz = TimeZone.getTimeZone("UTC")
    sdf.setTimeZone(tz)
    sdf.format(now)
  }

  lazy val providerToken: Option[URI] => String = { (uri: Option[URI]) =>
    {
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
    val dplaId = fromJsonString(record.sidecar) \ "dplaId" match {
      case JString(js) => js
      case _ => throw new RuntimeException("DPLA ID is not in metadata sidecar")
    }
    val _id = fromJsonString(record.sidecar) \ "prehashId" match {
      case JString(js) => js
      case _ =>
        throw new RuntimeException("Prehash ID is not in metadata sidecar")
    }

    val dplaMapJsonLd: JObject =
      ("_type" -> "item") ~
        ("_id" -> _id) ~
        ("_source" ->
          ("id" -> dplaId) ~
          ("_id" -> _id) ~
          ("@context" -> "http://dp.la/api/items/context") ~
          ("@id" -> record.dplaUri.toString) ~
          ("aggregatedCHO" -> "#sourceResource") ~
          ("dataProvider" ->
            ("@id" -> record.dataProvider.uri
              .getOrElse(throw new RuntimeException("Invalid dataProvider URI"))
              .toString) ~
            ("name" -> record.dataProvider.name) ~
            ("exactMatch" -> record.dataProvider.exactMatch.map(_.toString))) ~
          ("iiifManifest" -> record.iiifManifest
            .map(i => i.toString)) ~ // IIIF Manifest URI
          ("ingestDate" -> ingestDate) ~
          ("ingestType" -> "item") ~
          ("intermediateProvider" -> record.intermediateProvider
            .map(p => p.name)) ~
          ("isShownAt" -> record.isShownAt.uri.toString) ~
          ("mediaMaster" -> record.mediaMaster.map { m =>
            m.uri.toString
          }) ~ // full size media
          ("object" -> record.`preview`.map { o =>
            o.uri.toString
          }) ~ // in dpla map 3, object is the thumbnail
          ("rights" -> record.edmRights.map(r => r.toString)) ~
          ("originalRecord" ->
            ("stringValue" -> record.originalRecord)) ~ // work around b/c original record viewer in CQA
          // expects JSON and all ORs in ingest1 were converted to JSON
          // TODO We need to prettify this so the OR is readable in CQA
          ("provider" ->
            ("@id" -> record.provider.uri
              .getOrElse(throw new RuntimeException("Invalid Provider URI"))
              .toString) ~
            ("name" -> record.provider.name) ~
            ("exactMatch" -> record.provider.exactMatch.map(_.toString))) ~
          ("sourceResource" ->
            ("@id" ->
              (record.dplaUri.toString + "#SourceResource")) ~
            ("collection" ->
              record.sourceResource.collection.map { c =>
                ("title" -> c.title) ~
                  ("description" -> c.description)
              }) ~
            ("contributor" -> record.sourceResource.contributor.map { c =>
              c.name
            }) ~
            ("creator" -> record.sourceResource.creator.map { c => c.name }) ~
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
              // FIXME The SkosConcept object needs refinement in MAP5.1 to address some of the modeling issues
              // 1. Tracking ISO-639 term abbreviations
              // 2. Recording ISO-639 term labels
              // 3. Recording unenriched original value
              record.sourceResource.language.map { lang =>
                // If lang was enriched then the concept property has been set and it should be mapped to the name
                // property in ElasticSearch. If the lang value could not be enriched then use the original provided
                // value. Always map the enriched concept label to the iso639_3 field.
                ("name" -> lang.concept.getOrElse(
                  lang.providedLabel.getOrElse("")
                )) ~
                  ("iso639_3" -> lang.concept)
              }) ~
            ("publisher" -> record.sourceResource.publisher.map { p =>
              p.name
            }) ~
            ("relation" ->
              record.sourceResource.relation.map { _.value }) ~
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
            ("subject" -> record.sourceResource.subject.map { subject =>
              ("name" -> subject.providedLabel) ~
                ("scheme" -> subject.scheme.map(_.toString)) ~
                ("exactMatch" -> subject.exactMatch.map(_.toString))
            }) ~
            ("temporal" ->
              record.sourceResource.temporal.map { t =>
                ("displayDate" -> t.originalSourceDate) ~
                  ("begin" -> t.begin) ~
                  ("end" -> t.end)
              }) ~
            ("title" -> record.sourceResource.title) ~
            ("type" -> record.sourceResource.`type`)) ~
          ("@type" -> "ore:Aggregation") ~
          ("tags" -> record.tags.map { _.toString }))

    compact(render(dplaMapJsonLd))
  }

  def buildWikiMarkup(record: OreAggregation): String = {
    val dataProviderWikiUri = getDataProviderWikiId(record)
    val providerWikiUri = getProviderWikiId(record)
    val dplaId = getDplaId(record)
    val permissionsTemplate = getWikiPermissionTemplate(record.edmRights)
    val permissions = record.edmRights.toString match {
      case pt if pt.contains("http://rightsstatements.org") =>
        s"$permissionsTemplate | $dataProviderWikiUri"
      case _ => permissionsTemplate
    }

    s"""|== {{int:filedesc}} ==
        | {{ Artwork
        |   | Other fields 1 = {{ InFi | Creator | ${record.sourceResource.creator
         .flatMap { _.name }
         .map(escapeWikiChars)
         .mkString("; ")} }}
        |   | title = ${record.sourceResource.title
         .map(escapeWikiChars)
         .mkString("; ")}
        |   | description = ${record.sourceResource.description
         .map(escapeWikiChars)
         .mkString("; ")}
        |   | date = ${record.sourceResource.date
         .flatMap { _.prefLabel }
         .map(escapeWikiChars)
         .mkString("; ")}
        |   | permission = {{$permissions}}
        |   | source = {{ DPLA
        |       | ${escapeWikiChars(dataProviderWikiUri)}
        |       | hub = ${escapeWikiChars(providerWikiUri)}
        |       | url = ${escapeWikiChars(record.isShownAt.uri.toString)}
        |       | dpla_id = $dplaId
        |       | local_id = ${record.sourceResource.identifier
         .map(escapeWikiChars)
         .mkString("; ")}
        |   }}
        |   | Institution = {{ Institution | wikidata = $dataProviderWikiUri }}
        | }}""".stripMargin
  }

  private def getWikiPermissionTemplate(edmRights: Option[URI]): String = {
    edmRights match {
      case Some(uri) =>
        uri.toString match {
          case t if t.startsWith("http://rightsstatements.org/vocab/NKC/") =>
            "NKC"
          case t if t.startsWith("http://rightsstatements.org/vocab/NoC-US/") =>
            "NoC-US"
          case t
              if t.startsWith(
                "http://creativecommons.org/publicdomain/mark/"
              ) =>
            "PD-US"
          case t
              if t.startsWith(
                "http://creativecommons.org/publicdomain/zero/"
              ) =>
            "cc-zero"
          case t if t.startsWith("http://creativecommons.org/licenses/by/") =>
            licenseToMarkupCode(t)
          case t
              if t.startsWith("http://creativecommons.org/licenses/by-sa/") =>
            licenseToMarkupCode(t)
          case _ => ""
        }
      case _ => ""
    }
  }

  def licenseToMarkupCode(str: String): String = {
    val regex = raw"(^http://creativecommons.org/licenses/)(.*)".r
    str match {
      case regex(_, port) => s"Cc-${port.replaceAll("/", "-").dropRight(1)}"
      case _              => ""
    }
  }

  /** Escape reserved Wiki markup characters
    *
    * @param string
    * @return
    */
  def escapeWikiChars(string: String): String = {
    @annotation.tailrec
    def escapeWikiCharsAcc(list: List[String], value: String): String =
      list match {
        case Nil => value
        case h :: t =>
          escapeWikiCharsAcc(t, value.replace(h, escapeHelper(h)))
      }

    escapeWikiCharsAcc(reservedWikiChars, string)
  }

  private def escapeHelper(chars: String) = s"<nowiki>$chars</nowiki>"

  private def getDplaId(record: OreAggregation): String =
    fromJsonString(record.sidecar) \ "dplaId" match {
      case JString(js) => js
      case _ => throw new RuntimeException("DPLA ID is not in metadata sidecar")
    }

  private def getDataProviderWikiId(record: OreAggregation): String =
    getWikiId(record.dataProvider) match {
      case Some(uri) => uri
      case None =>
        throw new RuntimeException(
          s"dataProvider ${record.dataProvider.name.getOrElse("__MISSING__")} " +
            s"in ${getDplaId(record)} does not have wiki identifier "
        )
    }

  private def getProviderWikiId(record: OreAggregation): String =
    getWikiId(record.provider) match {
      case Some(uri) => uri
      case None =>
        throw new RuntimeException(
          s"provider ${record.provider.name.getOrElse("__MISSING__")} " +
            s"in ${getDplaId(record)} does not have wiki identifier "
        )
    }

  val WIKIDATA_ENTITY_PREFIX = "http://www.wikidata.org/entity/"

  private def getWikiId(agent: EdmAgent): Option[String] =
    agent.exactMatch
      .map(_.toString)
      .find(_.startsWith(WIKIDATA_ENTITY_PREFIX)) match {
      case Some(uri) => Some(uri.replace(WIKIDATA_ENTITY_PREFIX, ""))
      case None => None
    }


  // Taken from
  // https://stackoverflow.com/questions/40128816/remove-json-field-when-empty-value-in-serialize-with-json4s

  // implicit val formats: Formats = jackson.Serialization.formats(NoTypeHints).skippingEmptyValues

  implicit val formats: Formats =
    DefaultFormats.withEmptyValueStrategy(new EmptyValueStrategy {
      def noneValReplacement: Option[Nothing] = None

      def replaceEmpty(value: JValue): JValue = value match {
        case JString("") => JNothing
        case JArray(items) =>
          if (items.isEmpty) JNothing
          else JArray(items.map(replaceEmpty))
        case JObject(fields) =>
          JObject(fields map { case JField(name, v) =>
            JField(name, value = replaceEmpty(v))
          })
        case oth => oth
      }
    })

  lazy val avroSchema: Schema = new Schema.Parser()
    .parse(new FlatFileIO().readFileAsString("/avro/MAPRecord.avsc"))

  lazy val harvestAvroSchema: Schema = new Schema.Parser()
    .parse(new FlatFileIO().readFileAsString("/avro/OriginalRecord.avsc"))

  private lazy val harvestMimeTypeSchema: Schema =
    harvestAvroSchema.getField("mimetype").schema()

  lazy val sparkSchema: StructType =
    SchemaConverters.toSqlType(avroSchema).dataType.asInstanceOf[StructType]

  lazy val AVRO_MIME_JSON =
    new GenericData.EnumSymbol(harvestMimeTypeSchema, "application_json")

  lazy val AVRO_MIME_XML =
    new GenericData.EnumSymbol(harvestMimeTypeSchema, "application_xml")


  def toJsonString(json: JValue): String = compact(render(json))
  def fromJsonString(jsonString: String): JValue = parse(jsonString)

}
