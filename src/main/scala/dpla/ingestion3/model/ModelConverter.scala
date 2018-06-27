package dpla.ingestion3.model

import java.net.URI

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
    dplaUri = requiredUri(row, 0),
    sourceResource = toSourceResource(row.getStruct(1)),
    dataProvider = toEdmAgent(row.getStruct(2)),
    originalRecord = requiredString(row, 3),
    hasView = toRows(row, 4).map(toEdmWebResource),
    intermediateProvider = Option(row.getStruct(5)).map(toEdmAgent),
    isShownAt = toEdmWebResource(row.getStruct(6)),
    `object` = toOptionEdmWebResource(row.getStruct(7)),
    preview = toOptionEdmWebResource(row.getStruct(8)),
    provider = toEdmAgent(row.getStruct(9)),
    edmRights = optionalUri(row, 10),
    sidecar = optionalJValue(row, 11),
    messages = toMulti(row, 12, toIngestMessage)
  )

  private[model] def toSourceResource(row: Row): DplaSourceResource = DplaSourceResource(
    alternateTitle = stringSeq(row, 0),
    collection = toMulti(row, 1, toDcmiTypeCollection),
    contributor = toMulti(row, 2, toEdmAgent),
    creator = toMulti(row, 3, toEdmAgent),
    date = toMulti(row, 4, toEdmTimeSpan),
    description = stringSeq(row, 5),
    extent = stringSeq(row, 6),
    format = stringSeq(row, 7),
    genre = toMulti(row, 8, toSkosConcept),
    identifier = stringSeq(row, 9),
    language = toMulti(row, 10, toSkosConcept),
    place = toMulti(row, 11, toDplaPlace),
    publisher = toMulti(row, 12, toEdmAgent),
    relation = toMulti(row, 13, toLiteralOrUri),
    replacedBy = stringSeq(row, 14),
    replaces = stringSeq(row, 15),
    rights = stringSeq(row, 16),
    rightsHolder = toMulti(row, 17, toEdmAgent),
    subject = toMulti(row, 18, toSkosConcept),
    temporal = toMulti(row, 19, toEdmTimeSpan),
    title = stringSeq(row, 20),
    `type` = stringSeq(row, 21)
  )

  private[model] def toDplaPlace(row: Row): DplaPlace = DplaPlace(
    name = optionalString(row, 0),
    city = optionalString(row, 1),
    county = optionalString(row, 2),
    region = optionalString(row, 3),
    state = optionalString(row, 4),
    country = optionalString(row, 5),
    coordinates = optionalString(row, 6)
  )

  private[model] def toSkosConcept(row: Row): SkosConcept = SkosConcept(
    concept = optionalString(row, 0),
    providedLabel = optionalString(row, 1),
    note = optionalString(row, 2),
    scheme = optionalUri(row, 3),
    exactMatch = uriSeq(row, 4),
    closeMatch = uriSeq(row, 5)
  )

  private[model] def toEdmTimeSpan(row: Row): EdmTimeSpan = EdmTimeSpan(
    originalSourceDate = optionalString(row, 0),
    prefLabel = optionalString(row, 1),
    begin = optionalString(row, 2),
    end = optionalString(row, 3)
  )

  private[model] def toDcmiTypeCollection(row: Row): DcmiTypeCollection = DcmiTypeCollection(
    title = optionalString(row, 0),
    description = optionalString(row, 1)
  )

  private[model] def toOptionEdmWebResource(row: Row): Option[EdmWebResource] =
    Option(row) match {
      case None => None
      case Some(row) => Some(toEdmWebResource(row))
    }

  private[model] def toEdmWebResource(row: Row): EdmWebResource = EdmWebResource(
    uri = requiredUri(row, 0),
    fileFormat = stringSeq(row, 1),
    dcRights = stringSeq(row, 2),
    edmRights = optionalString(row, 3)
  )

  private[model] def toLiteralOrUri(row: Row): LiteralOrUri =
    if (row.getBoolean(1)) Right(new URI(row.getString(0)))
    else Left(row.getString(0))

  private[model] def toEdmAgent(row: Row): EdmAgent = EdmAgent(
    uri = optionalUri(row, 0),
    name = optionalString(row, 1),
    providedLabel = optionalString(row, 2),
    note = optionalString(row, 3),
    scheme = optionalUri(row, 4),
    exactMatch = uriSeq(row, 5),
    closeMatch = uriSeq(row, 6)
  )

  private[model] def toIngestMessage(row: Row): IngestMessage = IngestMessage(
    message = requiredString(row, 0),
    level = requiredString(row, 1),
    id = requiredString(row, 2),
    field = requiredString(row, 3),
    value = requiredString(row, 4)
    // enrichedValue = optionalString(row, 5) // TODO Fixup and add back
  )

  private[model] def toMulti[T](row: Row, fieldPosition: Integer, f: (Row) => T): Seq[T] = {
    toRows(row, fieldPosition).map(f)
  }

  private[model] def toRows(row: Row, fieldPosition: Integer): Seq[Row] = {
    row.getSeq[Row](fieldPosition)
  }

  private[model] def requiredUri(row: Row, fieldPosition: Integer): URI =
    optionalUri(row, fieldPosition)
      .getOrElse(throw new RuntimeException(s"Couldn't parse URI in row $row, field position $fieldPosition"))

  private[model] def optionalUri(row: Row, fieldPosition: Integer): Option[URI] =
    Option(row.getString(fieldPosition)).map(new URI(_))

  private[model] def requiredString(row: Row, fieldPosition: Integer): String =
    optionalString(row, fieldPosition)
      .getOrElse(throw new RuntimeException(s"Couldn't retrieve string in row $row, field position $fieldPosition"))

  private[model] def optionalString(row: Row, fieldPosition: Integer): Option[String] =
    Option(row.getString(fieldPosition))

  private[model] def stringSeq(row: Row, fieldPosition: Integer): Seq[String] =
    row.getSeq[String](fieldPosition)

  private[model] def uriSeq(row: Row, fieldPosition: Integer): Seq[URI] =
    stringSeq(row, fieldPosition).map(new URI(_))

  private[model] def optionalJValue(row: Row, fieldPosition: Integer): JValue =
    Try { parse(row.getString(fieldPosition)) } match {
      case Success(jv) => jv
      case Failure(_) => JNothing
    }
}