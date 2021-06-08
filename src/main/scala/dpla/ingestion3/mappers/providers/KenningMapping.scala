package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.filters.{DigitalSurrogateBlockList, FormatTypeValuesBlockList}
import dpla.ingestion3.mappers.utils.{Document, JsonExtractor, JsonMapping}
import dpla.ingestion3.model.DplaMapData.{AtLeastOne, ExactlyOne, ZeroToMany, ZeroToOne}
import dpla.ingestion3.model.{DcmiTypeCollection, DplaPlace, EdmAgent, EdmTimeSpan, EdmWebResource, SkosConcept, URI, nameOnlyAgent, nameOnlyCollection, nameOnlyConcept, nameOnlyPlace, stringOnlyTimeSpan, stringOnlyWebResource}
import dpla.ingestion3.utils.Utils
import org.json4s
import org.json4s.JsonAST.JString
import org.json4s.{JArray, JValue}
import org.json4s.JsonDSL._


class KenningMapping extends JsonMapping with JsonExtractor {

  val formatBlockList: Set[String] =
    DigitalSurrogateBlockList.termList ++
      FormatTypeValuesBlockList.termList

  // ID minting functions
  override def useProviderName: Boolean = false

  // Hard coded to prevent accidental changes to base ID
  override def getProviderName: String = "kenning"

  // Filename,Title,Description,Date,Subject,Rights,Creator

  override def originalId(implicit data: Document[JValue]): ZeroToOne[String] = data.get match {
    case JArray(List(JString(filename), _*)) => Some(filename)
    case _ => None
  }

  override def dplaUri(data: Document[JValue]): ZeroToOne[URI] =
    mintDplaItemUri(data)

  override def sidecar(data: Document[JValue]): JValue =
    ("prehashId", buildProviderBaseId()(data)) ~ ("dplaId", mintDplaId(data))

  override def dataProvider(data: Document[JValue]): ZeroToMany[EdmAgent] =
    Seq(nameOnlyAgent("Kenning Arlitsch Personal Photographs Collection"))

  override def originalRecord(data: Document[JValue]): ExactlyOne[String] =
    Utils.formatJson(data)

  override def provider(data: Document[JValue]): ExactlyOne[EdmAgent] =
    EdmAgent(
      name = Some("Montana State University"),
      uri = Some(URI("http://dp.la/api/contributor/msu"))
    )

  //  override def preview(data: Document[JValue]): ZeroToMany[EdmWebResource] =
  //    extractStrings(unwrap(data) \ "item" \ "resource" \ "image")
  //      .map(stringOnlyWebResource)
  //

  //todo
  override def isShownAt(data: Document[JValue]): ZeroToMany[EdmWebResource] =
    Seq(stringOnlyWebResource("http://google.com"))

  //  override def isShownAt(data: Document[JValue]): ZeroToMany[EdmWebResource] =
  //  // item['url']
  //    extractStrings(unwrap(data) \ "item" \ "url").map(stringOnlyWebResource)
  //

  override def collection(data: Document[JValue]): ZeroToMany[DcmiTypeCollection] =
    Seq(nameOnlyCollection("Kenning Arlitsch Personal Photographs Collection"))

  override def contributor(data: Document[JValue]): ZeroToMany[EdmAgent] =
    Seq(nameOnlyAgent("Kenning Arlitsch"))

  // Filename,Title,Description,Date,Subject,Rights,Creator

  override def date(data: Document[JValue]): ZeroToMany[EdmTimeSpan] = data.get match {
    case JArray(List(_, _, _, JString(date), _*)) => Seq(stringOnlyTimeSpan(date))
    case _ => Seq()
  }

  override def description(data: Document[JValue]): ZeroToMany[String] = data.get match {
    case JArray(List(_, _, JString(description), _*)) => Seq(description)
    case _ => Seq()
  }

  override def format(data: Document[JValue]): ZeroToMany[String] =
    Seq("Photograph")

  override def subject(data: Document[JValue]): ZeroToMany[SkosConcept] = data.get match {
    case JArray(List(_, _, _, _, JString(subject), _*)) =>
      subject
        .split(';')
        .map(_.trim)
        .map(nameOnlyConcept)
        .seq
    case _ => Seq()
  }

  override def title(data: Document[JValue]): AtLeastOne[String] = data.get match {
    case JArray(List(_, JString(title), _*)) => Seq(title)
    case _ => Seq()
  }

  override def `type`(data: Document[JValue]): ZeroToMany[String] = Seq("Image")

  override def edmRights(data: Document[JValue]): ZeroToMany[URI] = Seq(URI("http://creativecommons.org/licenses/by-nc-sa/3.0/us/"))


}
