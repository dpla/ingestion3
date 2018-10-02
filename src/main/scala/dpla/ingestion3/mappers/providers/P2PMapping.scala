package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.{Document, IdMinter, JsonExtractor, Mapping}
import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model.{EdmAgent, _}
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonAST.JString
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

class P2PMapping() extends Mapping[JValue] with IdMinter[JValue] with JsonExtractor {

  // ID minting functions
  override def useProviderName: Boolean = true

  // Hard coded to prevent accidental changes to base ID
  override def getProviderName: String = "p2p"

  override def getProviderId(implicit data: Document[JValue]): String =
    (data.get \ "@graph").children.flatMap(elem => {
      elem \ "@type" match {
        case JString("ore:Aggregation") => extractString("@id")(elem)
        case _ => None
      }
    }).headOption
      .getOrElse(throw new RuntimeException(s"No ID for record: ${compact(data)}"))

  // OreAggregation fields
  override def dplaUri(data: Document[JValue]): ExactlyOne[URI] = mintDplaItemUri(data)

  override def dataProvider(data: Document[JValue]): ZeroToMany[EdmAgent] =
    extractStringsDeep("edm:dataProvider")(data)
      .map(nameOnlyAgent)

  override def edmRights(data: Document[JValue]): ZeroToMany[URI] =
    extractStrings(data.get \ "@graph" \ "edm:rights" \ "@id").map(URI)

  override def isShownAt(data: Document[JValue]): ZeroToMany[EdmWebResource] =
    extractStrings(data.get \ "@graph" \ "edm:isShownAt" \ "@id").map(stringOnlyWebResource)

  override def originalRecord(data: Document[JValue]): ExactlyOne[String] = Utils.formatJson(data)

  override def preview(data: Document[JValue]): ZeroToMany[EdmWebResource] =
    extractStrings(data.get \ "@graph" \ "edm:preview" \ "@id").map(stringOnlyWebResource)

  override def provider(data: Document[JValue]): ExactlyOne[EdmAgent] = EdmAgent(
    name = Some("Plains to Peaks Collective"),
    uri = Some(URI("http://dp.la/api/contributor/p2p"))
  )

  override def sidecar(data: Document[JValue]): JValue =
    ("prehashId", buildProviderBaseId()(data)) ~ ("dplaId", mintDplaId(data))

  // SourceResource
  override def alternateTitle(data: Document[JValue]): ZeroToMany[String] =
    extractStringsDeep("dcterm:alternative")(data)

  override def contributor(data: Document[JValue]): ZeroToMany[EdmAgent] =
    extractStringsDeep("dcterm:contributor")(data).map(nameOnlyAgent)

  override def creator(data: Document[JValue]): ZeroToMany[EdmAgent] =
    extractStringsDeep("dcterm:creator")(data).map(nameOnlyAgent)

  override def date(data: Document[JValue]): ZeroToMany[EdmTimeSpan] =
    extractStringsDeep("dc:date")(data).map(stringOnlyTimeSpan)

  override def description(data: Document[JValue]): ZeroToMany[String] =
    extractStringsDeep("dcterm:description")(data)

  override def extent(data: Document[JValue]): ZeroToMany[String] =
    extractStringsDeep("dcterm:extent")(data)

  override def identifier(data: Document[JValue]): ZeroToMany[String] =
    extractStringsDeep("dcterm:identifier")(data)

  override def language(data: Document[JValue]): ZeroToMany[SkosConcept] =
    extractStringsDeep("dcterm:language")(data).map(nameOnlyConcept)

  override def subject(data: Document[JValue]): ZeroToMany[SkosConcept] =
    extractStringsDeep("dcterm:subject")(data)
      .map(nameOnlyConcept)

  override def title(data: Document[JValue]): AtLeastOne[String] =
    extractStringsDeep("dcterm:title")(data)
}
