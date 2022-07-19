package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.filters.{DigitalSurrogateBlockList, ExtentIdentificationList, FormatTypeValuesBlockList}
import dpla.ingestion3.mappers.utils.{Document, JsonExtractor, JsonMapping}
import dpla.ingestion3.model.DplaMapData.{AtLeastOne, ExactlyOne, ZeroToMany, ZeroToOne}
import dpla.ingestion3.model.{DcmiTypeCollection, DcmiTypes, DplaPlace, EdmAgent, EdmTimeSpan, EdmWebResource, LiteralOrUri, SkosConcept, URI, eitherStringOrUri, emptyEdmAgent, emptyJValue, emptySeq, emptyString, nameOnlyAgent, nameOnlyCollection, nameOnlyConcept, nameOnlyPlace, stringOnlyTimeSpan, uriOnlyWebResource}
import dpla.ingestion3.utils.Utils
import org.json4s
import org.json4s.JValue
import org.json4s.JsonAST.{JArray, JObject, JString, JValue}

abstract class JsonRetconMapper extends JsonMapping with JsonExtractor {

  // OreAggregation
  override def dplaUri(data: Document[JValue]): ZeroToOne[URI] = {
    data.get \ "_source" \ "@id" match {
      case JString(value) => Some(URI(f"http://dp.la/api/items/$value"))
      case _ => None
    }
  }

  override def dataProvider(data: Document[JValue]): ZeroToMany[EdmAgent] =
    extractStrings(data.get \ "_source" \ "dataProvider")
      .map(nameOnlyAgent)

  override def originalRecord(data: Document[JValue]): ExactlyOne[String] =
    extractStrings(data.get \ "_source" \ "originalRecord")
      .headOption
      .getOrElse("")

  override def isShownAt(data: Document[JValue]): ZeroToMany[EdmWebResource] =
    extractStrings(data.get \ "_source" \ "isShownAt")
      .map(URI)
      .map(uriOnlyWebResource)


  //todo: object vs preview
  override def `object`(data: Document[JValue]): ZeroToMany[EdmWebResource] =
    extractStrings(data.get \ "_source" \ "object")
      .map(URI)
      .map(uriOnlyWebResource)

  override def provider(data: Document[JValue]): ExactlyOne[EdmAgent] =
    extractStrings(data.get \ "_source" \ "provider" \ "name")
      .map(nameOnlyAgent)
      .headOption
      .getOrElse(EdmAgent())

  override def edmRights(data: Document[JValue]): ZeroToMany[URI] =
    extractStrings(data.get \ "_source" \ "rights")
      .map(URI)


  // this function deals with the fact that ingestion1 was a pile
  // that let you emit anything
  private def maybeArray[T](
                             value: JValue,
                             f: List[(String, JValue)] => Option[T]
                           ): Seq[T] = value match {
    case JObject(fields) =>
      f(fields).toSeq
    case JArray(values) =>
      values.flatMap({case JObject(fields) => f(fields)})
    case _ =>
      Seq()
  }

  // SourceResource
  override def collection(data: Document[JValue]): ZeroToMany[DcmiTypeCollection] =
    maybeArray(
      data.get \ "_source" \ "sourceResource" \ "collection",
      (fields) => {
        val title = fields.find(_._1 == "title").map(_._2.toString)
        val description = fields.find(_._1 == "description").map(_._2.toString)
        if (title.nonEmpty) Some(DcmiTypeCollection(title, description))
        else None
      }
    )

  override def contributor(data: Document[JValue]): ZeroToMany[EdmAgent] =
    extractStrings(data.get \ "_source" \ "sourceResource" \ "contributor")
      .map(nameOnlyAgent)

  override def creator(data: Document[JValue]): ZeroToMany[EdmAgent] =
    extractStrings(data.get \ "_source" \ "sourceResource" \ "creator")
      .map(nameOnlyAgent)

  //todo will not work for artstor
  override def date(data: Document[JValue]): ZeroToMany[EdmTimeSpan] =
    maybeArray(
      data.get \ "_source" \ "sourceResource" \ "date",
      fields => {
        val begin = fields.find(_._1 == "begin").map(_._2.toString)
        val end = fields.find(_._1 == "end").map(_._2.toString)
        val displayDate = fields.find(_._1 == "displayDate").map(_._2.toString)

        if (begin.nonEmpty && end.nonEmpty && displayDate.nonEmpty) None
        else Some(
          EdmTimeSpan(
            originalSourceDate = displayDate,
            prefLabel = displayDate,
            begin = begin,
            end = end
          )
        )
      }
    )

  override def description(data: Document[JValue]): ZeroToMany[String] =
    maybeArray(
      data.get \ "_source" \ "sourceResource" \ "description",
      x => Option(x.toString)
    )

  override def extent(data: Document[JValue]): ZeroToMany[String] =
    extractStrings(data.get \ "_source" \ "sourceResource" \ "extent")

  override def format(data: Document[JValue]): ZeroToMany[String] =
    extractStrings(data.get \ "_source" \ "sourceResource" \ "format") ++
      extractStrings(data.get \ "_source" \ "sourceResource" \ "hasFormat")

  override def genre(data: Document[JValue]): ZeroToMany[SkosConcept] =
    extractStrings(data.get \ "_source" \ "sourceResource" \ "genre")
      .map(nameOnlyConcept)

  override def identifier(data: Document[JValue]): ZeroToMany[String] =
    extractStrings(data.get \ "_source" \ "sourceResource" \ "identifier")


  override def language(data: Document[JValue]): ZeroToMany[SkosConcept] = ???
  override def place(data: Document[JValue]): ZeroToMany[DplaPlace] = ???
  override def publisher(data: Document[JValue]): ZeroToMany[EdmAgent] = ???
  override def relation(data: Document[JValue]): ZeroToMany[LiteralOrUri] = ???
  override def replacedBy(data: Document[JValue]): ZeroToMany[String] = ???
  override def replaces(data: Document[JValue]): ZeroToMany[String] = ???
  override def rights(data: Document[JValue]): AtLeastOne[String] = ???
  override def rightsHolder(data: Document[JValue]): ZeroToMany[EdmAgent] = ???
  override def subject(data: Document[JValue]): ZeroToMany[SkosConcept] = ???
  override def temporal(data: Document[JValue]): ZeroToMany[EdmTimeSpan] = ???
  override def title(data: Document[JValue]): AtLeastOne[String] = ???
  override def `type`(data: Document[JValue]): ZeroToMany[String] = ???

}
