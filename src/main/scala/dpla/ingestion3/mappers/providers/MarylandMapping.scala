package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.mappers.utils.{Document, XmlMapping, XmlExtractor}
import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.xml._


class MarylandMapping extends XmlMapping with XmlExtractor {

  // ID minting functions
  override def useProviderName: Boolean = true

  override def getProviderName: String = "maryland"

  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] = ???

  // SourceResource mapping

  override def collection(data: Document[NodeSeq]): ZeroToMany[DcmiTypeCollection] = ???

  override def contributor(data: Document[NodeSeq]): ZeroToMany[EdmAgent] = ???

  override def creator(data: Document[NodeSeq]): ZeroToMany[EdmAgent] = ???

  override def date(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] = ???

  override def description(data: Document[NodeSeq]): ZeroToMany[String] = ???

  override def format(data: Document[NodeSeq]): ZeroToMany[String] = ???

  override def language(data: Document[NodeSeq]): ZeroToMany[SkosConcept] = ???

  override def publisher(data: Document[NodeSeq]): ZeroToMany[EdmAgent] = ???

  override def rights(data: Document[NodeSeq]): AtLeastOne[String] = ??? // NOT IN I1

  override def subject(data: Document[NodeSeq]): ZeroToMany[SkosConcept] = ???

  override def temporal(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] = ???

  override def title(data: Document[NodeSeq]): ZeroToMany[String] = ???

  override def `type`(data: Document[NodeSeq]): ZeroToMany[String] = ???

  // OreAggregation
  override def dplaUri(data: Document[NodeSeq]): ZeroToOne[URI] = mintDplaItemUri(data)

  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] = ???

  override def edmRights(data: Document[NodeSeq]): ZeroToMany[URI] = ???

  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] = ???

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] = Utils.formatXml(data)

  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] = ???

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = agent

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(data))

  // Helper method
  def agent = EdmAgent(
    name = Some("Digital Maryland"),
    uri = Some(URI("http://dp.la/api/contributor/maryland"))
  )
}
