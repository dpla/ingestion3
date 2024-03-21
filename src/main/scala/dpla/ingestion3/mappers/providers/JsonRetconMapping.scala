package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.{Document, JsonExtractor, JsonMapping}
import dpla.ingestion3.model.DplaMapData.{
  AtLeastOne,
  ExactlyOne,
  ZeroToMany,
  ZeroToOne
}
import dpla.ingestion3.model.{
  DcmiTypeCollection,
  DplaPlace,
  EdmAgent,
  EdmTimeSpan,
  EdmWebResource,
  LiteralOrUri,
  SkosConcept,
  URI,
  eitherStringOrUri,
  nameOnlyAgent,
  nameOnlyConcept,
  uriOnlyWebResource
}
import org.json4s
import org.json4s.JValue
import org.json4s.JsonAST.{JArray, JObject, JString}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, render}

abstract class JsonRetconMapping extends JsonMapping with JsonExtractor {

  // OreAggregation
  override def dplaUri(data: Document[JValue]): ZeroToOne[URI] = {
    data.get \ "_source" \ "id" match {
      case JString(value) => Some(URI(f"http://dp.la/api/items/$value"))
      case _              => None
    }
  }

  override def sidecar(data: Document[JValue]): JValue =
    ("prehashId", originalId(data)) ~
      ("dplaId", extractString(data.get \ "_source" \ "id").get)

  override def dataProvider(data: Document[JValue]): ZeroToMany[EdmAgent] =
    extractStrings(data.get \ "_source" \ "dataProvider")
      .map(nameOnlyAgent)

  override def originalRecord(data: Document[JValue]): ExactlyOne[String] =
    compact(render(data.get \ "_source" \ "originalRecord"))

  override def isShownAt(data: Document[JValue]): ZeroToMany[EdmWebResource] =
    extractStrings(data.get \ "_source" \ "isShownAt")
      .map(URI)
      .map(uriOnlyWebResource)

  override def `object`(data: Document[JValue]): ZeroToMany[EdmWebResource] =
    extractStrings(data.get \ "_source" \ "object")
      .map(URI)
      .map(uriOnlyWebResource)

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
      values.flatMap({ case JObject(fields) => f(fields) })
    case _ =>
      Seq()
  }

  // SourceResource
  override def collection(
      data: Document[JValue]
  ): ZeroToMany[DcmiTypeCollection] =
    maybeArray(
      data.get \ "_source" \ "sourceResource" \ "collection",
      (fields) => {
        val title = extractStrings("title")(fields).headOption
        val description = extractStrings("description")(fields).headOption
        if (title.nonEmpty)
          Some(
            DcmiTypeCollection(
              title = title,
              description = description,
              isShownAt = None
            )
          )
        else None
      }
    )

  override def contributor(data: Document[JValue]): ZeroToMany[EdmAgent] =
    extractStrings(data.get \ "_source" \ "sourceResource" \ "contributor")
      .map(nameOnlyAgent)

  override def creator(data: Document[JValue]): ZeroToMany[EdmAgent] =
    extractStrings(data.get \ "_source" \ "sourceResource" \ "creator")
      .map(nameOnlyAgent)

  private def extractEdmTimeSpan(
      fields: List[(String, JValue)]
  ): Option[EdmTimeSpan] = {
    {
      val begin = extractStrings("begin")(fields).headOption
      val end = extractStrings("end")(fields).headOption
      val displayDate = extractStrings("displayDate")(fields).headOption

      if (begin.isEmpty && end.isEmpty && displayDate.isEmpty) None
      else
        Some(
          EdmTimeSpan(
            originalSourceDate = displayDate,
            prefLabel = displayDate,
            begin = begin,
            end = end
          )
        )
    }
  }

  override def date(data: Document[JValue]): ZeroToMany[EdmTimeSpan] =
    maybeArray(
      data.get \ "_source" \ "sourceResource" \ "date",
      extractEdmTimeSpan
    )

  override def description(data: Document[JValue]): ZeroToMany[String] =
    extractStrings(data.get \ "_source" \ "sourceResource" \ "description")

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

  override def language(data: Document[JValue]): ZeroToMany[SkosConcept] =
    extractStrings(
      data.get \ "_source" \ "sourceResource" \ "language" \ "name"
    )
      .map(nameOnlyConcept)

  override def place(data: Document[JValue]): ZeroToMany[DplaPlace] =
    maybeArray(
      data.get \ "_source" \ "sourceResource" \ "spatial",
      (fields) => {
        val name = extractStrings("name")(fields).headOption
        val city = extractStrings("city")(fields).headOption
        val county = extractStrings("county")(fields).headOption
        val state = extractStrings("state")(fields).headOption
        val country = extractStrings("country")(fields).headOption
        val region = extractStrings("region")(fields).headOption
        val coordinates = extractStrings("coordinates")(fields).headOption

        if (
          Seq(
            name,
            city,
            county,
            state,
            country,
            region,
            coordinates
          ).flatten.isEmpty
        ) None
        else
          Some(
            DplaPlace(
              name = name,
              city = city,
              county = county,
              state = state,
              country = country,
              region = region,
              coordinates = coordinates
            )
          )
      }
    )

  override def publisher(data: Document[JValue]): ZeroToMany[EdmAgent] =
    extractStrings(data.get \ "_source" \ "sourceResource" \ "publisher")
      .map(nameOnlyAgent)

  override def relation(data: Document[JValue]): ZeroToMany[LiteralOrUri] =
    extractStrings(data.get \ "_source" \ "sourceResource" \ "relation")
      .map(eitherStringOrUri)

  override def rights(data: Document[JValue]): AtLeastOne[String] =
    extractStrings(data.get \ "_source" \ "sourceResource" \ "rights")

  override def subject(data: Document[JValue]): ZeroToMany[SkosConcept] =
    extractStrings(data.get \ "_source" \ "sourceResource" \ "subject" \ "name")
      .map(nameOnlyConcept)

  override def temporal(data: Document[JValue]): ZeroToMany[EdmTimeSpan] =
    maybeArray(
      data.get \ "_source" \ "sourceResource" \ "temporal",
      extractEdmTimeSpan
    )
  override def title(data: Document[JValue]): AtLeastOne[String] =
    extractStrings(data.get \ "_source" \ "sourceResource" \ "title")

  override def `type`(data: Document[JValue]): ZeroToMany[String] =
    extractStrings(data.get \ "_source" \ "sourceResource" \ "type")

}

class ArtstorRetconMapping extends JsonRetconMapping {
  override def useProviderName: Boolean = true
  override def getProviderName: Option[String] = Some("artstor")
  override def originalId(implicit
      data: Document[JValue]
  ): ZeroToOne[String] = {
    extractString("_id")(data).map(_.substring("artstor--".length))
  }

  override def provider(data: Document[JValue]): ExactlyOne[EdmAgent] =
    EdmAgent(
      name = Some("Artstor"),
      uri = Some(URI("http://dp.la/api/contributor/artstor"))
    )
}

class KentuckyRetconMapping extends JsonRetconMapping {
  override def useProviderName: Boolean = true
  override def getProviderName: Option[String] = Some("kentucky")
  override def originalId(implicit data: Document[JValue]): ZeroToOne[String] =
    extractString("_id")(data).map(_.substring("kentucky--".length))
  override def provider(data: Document[JValue]): ExactlyOne[EdmAgent] =
    EdmAgent(
      name = Some("Kentucky Digital Library"),
      uri = Some(URI("http://dp.la/api/contributor/kdl"))
    )
}

class LcRetconMapping extends JsonRetconMapping {
  override def useProviderName: Boolean = false
  override def getProviderName: Option[String] = Some("lc")
  override def originalId(implicit data: Document[JValue]): ZeroToOne[String] =
    extractString("_id")(data)
  override def provider(data: Document[JValue]): ExactlyOne[EdmAgent] =
    EdmAgent(
      name = Some("Library of Congress"),
      uri = Some(URI("http://dp.la/api/contributor/lc"))
    )
}

class MaineRetconMapping extends JsonRetconMapping {
  override def useProviderName: Boolean = true
  override def getProviderName: Option[String] = Some("maine")
  override def originalId(implicit data: Document[JValue]): ZeroToOne[String] =
    extractString("_id")(data).map(_.substring("maine--".length))
  override def provider(data: Document[JValue]): ExactlyOne[EdmAgent] =
    EdmAgent(
      name = Some("Digital Maine"),
      uri = Some(URI("http://dp.la/api/contributor/maine"))
    )
}

class WashingtonRetconMapping extends JsonRetconMapping {
  override def useProviderName: Boolean = false
  override def getProviderName: Option[String] = Some("washington")
  override def originalId(implicit data: Document[JValue]): ZeroToOne[String] =
    extractString("_id")(data)
  override def provider(data: Document[JValue]): ExactlyOne[EdmAgent] =
    EdmAgent(
      name = Some("University of Washington"),
      uri = Some(URI("http://dp.la/api/contributor/washington"))
    )

  override def tags(data: Document[json4s.JValue]): ZeroToMany[URI] =
    Seq(URI("nwdh"))
}
