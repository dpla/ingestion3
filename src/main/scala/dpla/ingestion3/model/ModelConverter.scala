package dpla.ingestion3.model

import dpla.ingestion3.messages.IngestMessage
import dpla.ingestion3.model.DplaMapData.LiteralOrUri
import org.apache.spark.sql.Row
import org.json4s.JsonAST.{JNothing, JValue}
import org.json4s.jackson.JsonMethods.parse

import scala.util.{Failure, Success, Try}

/**
  * Responsible for taking a Row representing a structure of fields that represents a DplaMapData in Avro or other
  * lower-level Spark output formats and turning it back into a DplaMapData.
  *
  * PLEASE NOTE: The field index values represented here are significant and cannot be changed without a concurrent
  * effort to rewrite all the existing data stored in our master dataset, and a matching change to RowConverter, which
  * is responsible for converting a DplaMapData to a Row and represents the opposite of this transformation.
  */

object ModelConverter {

  def toModel(row: Row): OreAggregation = OreAggregation(
    dplaUri = requiredUri(row, "dplaUri"),
    sourceResource = toSourceResource(row.getAs[Row]("SourceResource")),
    dataProvider = toEdmAgent(row.getAs[Row]("dataProvider")),
    originalRecord = requiredString(row, "originalRecord"),
    hasView = toRows(row, "hasView").map(toEdmWebResource),
    intermediateProvider = Option(row.getAs[Row]("intermediateProvider")).map(toEdmAgent),
    isShownAt = toEdmWebResource(row.getAs[Row]("isShownAt")),
    `object` = toOptionEdmWebResource(row.getAs[Row]("object")),
    preview = toOptionEdmWebResource(row.getAs[Row]("preview")),
    provider = toEdmAgent(row.getAs[Row]("provider")),
    edmRights = optionalUri(row, "edmRights"),
    sidecar = optionalJValue(row, "sidecar"),
    messages = toMulti(row, "messages", toIngestMessage),
    originalId = potentiallyMissingStringField(row, "originalId").getOrElse("MISSING")
  )

  private[model] def toSourceResource(row: Row): DplaSourceResource = DplaSourceResource(
    alternateTitle = stringSeq(row, "alternateTitle"),
    collection = toMulti(row, "collection", toDcmiTypeCollection),
    contributor = toMulti(row, "contributor", toEdmAgent),
    creator = toMulti(row, "creator", toEdmAgent),
    date = toMulti(row, "date", toEdmTimeSpan),
    description = stringSeq(row, "description"),
    extent = stringSeq(row, "extent"),
    format = stringSeq(row, "format"),
    genre = toMulti(row, "genre", toSkosConcept),
    identifier = stringSeq(row, "identifier"),
    language = toMulti(row, "language", toSkosConcept),
    place = toMulti(row, "place", toDplaPlace),
    publisher = toMulti(row, "publisher", toEdmAgent),
    relation = toMulti(row, "relation", toLiteralOrUri),
    replacedBy = stringSeq(row, "replacedBy"),
    replaces = stringSeq(row, "replaces"),
    rights = stringSeq(row, "rights"),
    rightsHolder = toMulti(row, "rightsHolder", toEdmAgent),
    subject = toMulti(row, "subject", toSkosConcept),
    temporal = toMulti(row, "temporal", toEdmTimeSpan),
    title = stringSeq(row, "title"),
    `type` = stringSeq(row, "type")
  )

  private[model] def toDplaPlace(row: Row): DplaPlace = DplaPlace(
    name = optionalString(row, "name"),
    city = optionalString(row, "city"),
    county = optionalString(row, "county"),
    region = optionalString(row, "region"),
    state = optionalString(row, "state"),
    country = optionalString(row, "country"),
    coordinates = optionalString(row, "coordinates")
  )

  private[model] def toSkosConcept(row: Row): SkosConcept = SkosConcept(
    concept = optionalString(row, "concept"),
    providedLabel = optionalString(row, "providedLabel"),
    note = optionalString(row, "note"),
    scheme = optionalUri(row, "scheme"),
    exactMatch = uriSeq(row, "exactMatch"),
    closeMatch = uriSeq(row, "closeMatch")
  )

  private[model] def toEdmTimeSpan(row: Row): EdmTimeSpan = EdmTimeSpan(
    originalSourceDate = optionalString(row, "originalSourceDate"),
    prefLabel = optionalString(row, "prefLabel"),
    begin = optionalString(row, "begin"),
    end = optionalString(row, "end")
  )

  private[model] def toDcmiTypeCollection(row: Row): DcmiTypeCollection = DcmiTypeCollection(
    title = optionalString(row, "title"),
    description = optionalString(row, "description")
  )

  private[model] def toOptionEdmWebResource(row: Row): Option[EdmWebResource] =
    Option(row) match {
      case None => None
      case Some(row) => Some(toEdmWebResource(row))
    }

  private[model] def toEdmWebResource(row: Row): EdmWebResource = EdmWebResource(
    uri = requiredUri(row, "uri"),
    fileFormat = stringSeq(row, "fileFormat"),
    dcRights = stringSeq(row, "dcRights"),
    edmRights = optionalString(row, "edmRights"),
    isReferencedBy = optionalUri(row, "isReferencedBy")
  )

  private[model] def toLiteralOrUri(row: Row): LiteralOrUri =
    if (row.getBoolean(1)) Right(new URI(row.getString(0)))
    else Left(row.getString(0))

  private[model] def toEdmAgent(row: Row): EdmAgent = EdmAgent(
    uri = optionalUri(row, "uri"),
    name = optionalString(row, "name"),
    providedLabel = optionalString(row, "providedLabel"),
    note = optionalString(row, "note"),
    scheme = optionalUri(row, "scheme"),
    exactMatch = uriSeq(row, "exactMatch"),
    closeMatch = uriSeq(row, "closeMatch")
  )

  private[model] def toIngestMessage(row: Row): IngestMessage = IngestMessage(
    message = requiredString(row, "message"),
    level = requiredString(row, "level"),
    id = requiredString(row, "id"),
    field = requiredString(row, "field"),
    value = requiredString(row, "value"),
    enrichedValue = requiredString(row, "enrichedValue") // TODO Fixup and add back
  )

  private[model] def toMulti[T](row: Row, fieldName: String, f: (Row) => T): Seq[T] = {
    toRows(row, fieldName).map(f)
  }

  private[model] def toRows(row: Row, fieldName: String): Seq[Row] = {
    row.getAs[Seq[Row]](fieldName)
  }

  private[model] def requiredUri(row: Row, fieldName: String): URI =
    optionalUri(row, fieldName)
      .getOrElse(throw new RuntimeException(s"Couldn't parse URI in row $row, field name $fieldName"))

  private[model] def optionalUri(row: Row, fieldName: String): Option[URI] = {
    Try{ Option(row.getAs[String](fieldName)).map(URI) } match {
      case Success(uriOpt) => uriOpt
      case Failure(_) => None
    }
  }

  private[model] def requiredString(row: Row, fieldName: String): String =
    optionalString(row, fieldName)
      .getOrElse(throw new RuntimeException(s"Couldn't retrieve string in row $row, field name $fieldName"))

  private[model] def optionalString(row: Row, fieldName: String): Option[String] =
    Option(row.getAs[String](fieldName))

  private[model] def stringSeq(row: Row, fieldName: String): Seq[String] =
    row.getAs[Seq[String]](fieldName)

  private[model] def uriSeq(row: Row, fieldName: String): Seq[URI] =
    stringSeq(row, fieldName).map(new URI(_))

  private[model] def optionalJValue(row: Row, fieldName: String): JValue =
    Try { parse(row.getAs[String](fieldName)) } match {
      case Success(jv) => jv
      case Failure(_) => JNothing
    }

  // Handle field index that may not be present in the data.
  private[model] def potentiallyMissingStringField(row: Row, fieldName: String): Option[String] = {
    Try { optionalString(row, fieldName) }.getOrElse(None)
  }
}
