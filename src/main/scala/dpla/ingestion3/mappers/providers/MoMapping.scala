package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.enrichments.normalizations.filters.ExtentIdentificationList
import dpla.ingestion3.mappers.utils._
import dpla.ingestion3.messages.IngestMessageTemplates
import dpla.ingestion3.model.DplaMapData.{AtLeastOne, ExactlyOne, ZeroToMany, ZeroToOne}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.json4s
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._

class MoMapping extends JsonMapping with JsonExtractor with IdMinter[JValue] with IngestMessageTemplates {

  val formatBlockList: Set[String] = ExtentIdentificationList.termList

  // ID minting functions
  override def useProviderName: Boolean = true

  override def getProviderName: String = "mo"

  override def getProviderId(implicit data: Document[JValue]): String =
    extractString(unwrap(data) \ "@id")
      .getOrElse(throw new RuntimeException(s"No ID for record: ${compact(data)}"))

  // OreAggregation
  override def dataProvider(data: Document[JValue]): ZeroToMany[EdmAgent] =
    extractStrings(unwrap(data) \ "dataProvider").map(nameOnlyAgent)

  override def dplaUri(data: Document[JValue]): ExactlyOne[URI] = URI(mintDplaId(data))

  override def hasView(data: Document[JValue]): ZeroToMany[EdmWebResource] =
    extractStrings(unwrap(data) \ "hasView" \ "@id").map(stringOnlyWebResource)

  override def isShownAt(data: Document[JValue]): ZeroToMany[EdmWebResource] =
    extractStrings(unwrap(data) \ "isShownAt").map(stringOnlyWebResource)

  override def `object`(data: Document[JValue]): ZeroToMany[EdmWebResource] =
    extractStrings(unwrap(data) \ "object").map(stringOnlyWebResource)

  override def originalRecord(data: Document[JValue]): ExactlyOne[String] =
    Utils.formatJson(data)

  override def provider(data: Document[JValue]): ExactlyOne[EdmAgent] = agent

  override def sidecar(data: Document[JValue]): JValue =
    ("prehashId", buildProviderBaseId()(data)) ~ ("dplaId", mintDplaId(data))

  def agent = EdmAgent(
    name = Some("Missouri Hub"),
    uri = Some(URI("http://dp.la/api/contributor/missouri-hub"))
  )
}
