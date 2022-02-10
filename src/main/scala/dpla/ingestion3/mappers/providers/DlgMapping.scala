package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.enrichments.normalizations.filters.{DigitalSurrogateBlockList, ExtentIdentificationList, FormatTypeValuesBlockList}
import dpla.ingestion3.mappers.utils.{Document, JsonExtractor, JsonMapping}
import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model.{EdmAgent, _}
import dpla.ingestion3.utils.Utils
import org.json4s
import org.json4s.JValue
import org.json4s.JsonDSL._

class DlgMapping extends JsonMapping with JsonExtractor {

  // ID minting functions
  override def useProviderName: Boolean = true

  // Hard coded to prevent accidental changes to base ID
  override def getProviderName: Option[String] = Some("dlg")

  // OreAggregation fields
  override def dplaUri(data: Document[JValue]): ZeroToOne[URI] = mintDplaItemUri(data)

  override def originalId(implicit data: Document[JValue]): ZeroToOne[String] =
    extractString("id")(data)

  override def sidecar(data: Document[JValue]): JValue =
    ("prehashId", buildProviderBaseId()(data)) ~ ("dplaId", mintDplaId(data))

  override def dataProvider(data: Document[JValue]): ZeroToMany[EdmAgent] =
    extractStrings("dcterms_provenance_display")(data)
      .map(nameOnlyAgent)

  override def edmRights(data: Document[JValue]): ZeroToMany[URI] =
    extractStrings("dc_right_display")(data)
      .map(URI)

  //  override def mediaMaster(data: Document[JValue]): ZeroToMany[EdmWebResource] = {
  //    extractStrings("edm_is_shown_by_display")(data).map(stringOnlyWebResource)
  //  }

  override def originalRecord(data: Document[JValue]): ExactlyOne[String] = Utils.formatJson(data)

  override def preview(data: Document[JValue]): ZeroToMany[EdmWebResource] =
    originalId(data)
      .map(id => s"https://dlg.galileo.usg.edu/do-th:$id")
      .map(stringOnlyWebResource)
      .toSeq

  override def iiifManifest(data: Document[json4s.JValue]): ZeroToMany[URI] = {
    extractStrings("iiif_manifest_url_ss")(data)
      .map(URI)
  }

  override def provider(data: Document[JValue]): ExactlyOne[EdmAgent] = EdmAgent(
    name = Some("Digital Library of Georgia"),
    uri = Some(URI("http://dp.la/api/contributor/dlg"))
  )

  override def isShownAt(data: Document[JValue]): ZeroToMany[EdmWebResource] =
    extractStrings("edm_is_shown_at_display")(data)
      .map(stringOnlyWebResource)

  // SourceResource
  override def collection(data: Document[JValue]): ZeroToMany[DcmiTypeCollection] =
    extractStrings("collection_titles_sms")(data)
      .map(nameOnlyCollection)

  override def contributor(data: Document[JValue]): ZeroToMany[EdmAgent] =
    extractStrings("dcterms_contributor_display")(data)
      .map(nameOnlyAgent)

  override def creator(data: Document[JValue]): ZeroToMany[EdmAgent] =
    extractStrings("dcterms_creator_display")(data)
      .map(nameOnlyAgent)

  override def date(data: Document[JValue]): ZeroToMany[EdmTimeSpan] =
    extractStrings("dc_date_display")(data)
      .map(stringOnlyTimeSpan)

  override def description(data: Document[JValue]): ZeroToMany[String] =
    extractStrings("dcterms_description_display")(data)

  override def extent(data: Document[JValue]): ZeroToMany[String] =
    extractStrings("dcterms_extent_display")(data)

  override def format(data: Document[JValue]): ZeroToMany[String] =
    extractStrings("dc_format_display")(data)
      .map(_.applyBlockFilter(
        DigitalSurrogateBlockList.termList ++
          FormatTypeValuesBlockList.termList ++
          ExtentIdentificationList.termList))
      .filter(_.nonEmpty)

  override def identifier(data: Document[JValue]): ZeroToMany[String] =
    extractStrings("dcterms_identifier_display")(data)

  override def language(data: Document[JValue]): ZeroToMany[SkosConcept] =
    extractStrings("dcterms_language_display")(data)
      .map(nameOnlyConcept)

  override def place(data: Document[JValue]): ZeroToMany[DplaPlace] =
    extractStrings("dcterms_spatial_display")(data)
      .map(nameOnlyPlace)

  override def publisher(data: Document[JValue]): ZeroToMany[EdmAgent] =
    extractStrings("dcterms_publisher_display")(data).map(nameOnlyAgent)

  override def relation(data: Document[JValue]): ZeroToMany[LiteralOrUri] =
    extractStrings("dc_relation_display")(data).map(eitherStringOrUri)

  override def rights(data: Document[JValue]): AtLeastOne[String] =
    extractStrings("dlg_local_right")(data) ++
      extractStrings("dlg_local_right_display")(data)

  override def rightsHolder(data: Document[JValue]): ZeroToMany[EdmAgent] =
    extractStrings("dcterms_rights_holder_display")(data)
      .map(nameOnlyAgent)

  override def subject(data: Document[JValue]): ZeroToMany[SkosConcept] =
    extractStrings("dcterms_subject_display")(data)
      .map(nameOnlyConcept)

  override def title(data: Document[JValue]): AtLeastOne[String] =
    extractStrings("dcterms_title_display")(data)

  override def `type`(data: Document[JValue]): ZeroToMany[String] =
    extractStrings("dcterms_type_display")(data)
}
