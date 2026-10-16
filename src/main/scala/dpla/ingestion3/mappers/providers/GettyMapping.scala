package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.enrichments.normalizations.filters.ExtentIdentificationList
import dpla.ingestion3.mappers.utils.{Document, JsonExtractor, JsonMapping}
import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model.{nameOnlyAgent, _}
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._

class GettyMapping extends JsonMapping with JsonExtractor {

  private val extentAllowList: Set[String] =
    ExtentIdentificationList.termList

  // ID minting functions
  override def useProviderName: Boolean = true
  override def getProviderName: Option[String] = Some("getty")

  override def originalId(implicit
      data: Document[JValue]
  ): ZeroToOne[String] = {
    extractStrings(unwrap(data) \ "pnx" \ "control" \ "recordid").headOption
  }

  // SourceResource mapping
  override def collection(data: Document[JValue]): Seq[DcmiTypeCollection] =
    // display/lds43 AND display/lds34
    (extractStrings(unwrap(data) \ "pnx" \ "display" \ "lds43") ++
      extractStrings(unwrap(data) \ "pnx" \ "display" \ "lds34"))
      .map(nameOnlyCollection)

  override def contributor(data: Document[JValue]): Seq[EdmAgent] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "contributor")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyAgent)

  override def creator(data: Document[JValue]): Seq[EdmAgent] =
    (extractStrings(unwrap(data) \ "pnx" \ "display" \ "creator") ++
      extractStrings(unwrap(data) \ "pnx" \ "display" \ "lds50"))
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyAgent)

  override def date(data: Document[JValue]): Seq[EdmTimeSpan] =
    // display/creationdate
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "creationdate")
      .map(stringOnlyTimeSpan)

  override def description(data: Document[JValue]): Seq[String] =
    // display/lds04 AND display/lds28 AND display/format
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "lds04") ++
      extractStrings(unwrap(data) \ "pnx" \ "display" \ "lds28") ++
      extractStrings(unwrap(data) \ "pnx" \ "display" \ "format")

  override def format(data: Document[JValue]): ZeroToMany[String] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "lds09")
      .map(_.applyBlockFilter(extentAllowList))
      .filter(_.nonEmpty)

  override def identifier(data: Document[JValue]): Seq[String] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "lds14")
      .flatMap(_.splitAtDelimiter(";"))

  override def language(data: Document[JValue]): Seq[SkosConcept] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "language")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyConcept)

  override def place(data: Document[JValue]): Seq[DplaPlace] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "coverage")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyPlace)

  override def publisher(data: Document[JValue]): Seq[EdmAgent] =
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "publisher")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyAgent)

  /** Promote a rights URI Getty has already published into edmRights.
    *
    * Getty publishes free-text rights statements, not standardized URIs: across
    * the 2026-09-12 harvest exactly two of 101,384 records carry one
    * (http://rightsstatements.org/vocab/InC/1.0/, in lds27). Those two were
    * reaching DPLA as free-text rights -- a URI sitting in a text field, where it
    * cannot be used as a rights statement.
    *
    * A scan of every field in every record confirmed Getty has no separate
    * rights-URI field, so the only place a published URI can appear is alongside
    * the free text.
    *
    * This deliberately does NOT crosswalk Getty's local statements. Getty's
    * wording (the Open Content Program statement covers 77.5% of the hub) is a
    * partner statement whose meaning is theirs to declare; assigning a URI to it
    * would change what the record says. Non-standard values stay in free-text
    * rights, untouched.
    *
    * The match is exact -- the whole value must be a valid edmRights URI, not
    * merely contain one -- so a URI mentioned inside prose is never harvested out
    * of context.
    */
  override def edmRights(data: Document[JValue]): ZeroToMany[URI] =
    (extractStrings(unwrap(data) \ "pnx" \ "display" \ "rights") ++
      extractStrings(unwrap(data) \ "pnx" \ "display" \ "lds27"))
      .map(_.trim)
      .map(URI)
      .filter(_.isValidEdmRightsUri)
      .distinct

  override def rights(data: Document[JValue]): AtLeastOne[String] =
    // display/lds27 AND display/rights
    extractStrings(
      (unwrap(data) \ "pnx" \ "display" \ "rights") ++
        extractStrings(unwrap(data) \ "pnx" \ "display" \ "lds27")
    )

  override def subject(data: Document[JValue]): Seq[SkosConcept] =
    // display/subject AND display/lds49
    (extractStrings(unwrap(data) \ "pnx" \ "display" \ "subject") ++
      extractStrings(unwrap(data) \ "pnx" \ "display" \ "lds49"))
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyConcept)

  override def title(data: Document[JValue]): Seq[String] =
    // display/title AND display/lds03
    (extractStrings(unwrap(data) \ "pnx" \ "display" \ "title") ++
      extractStrings(unwrap(data) \ "pnx" \ "display" \ "lds03"))
      .flatMap(_.splitAtDelimiter(";"))

  override def `type`(data: Document[JValue]): Seq[String] =
    // display/lds26
    extractStrings(unwrap(data) \ "pnx" \ "display" \ "lds26")

  // OreAggregation
  override def dplaUri(data: Document[JValue]): ZeroToOne[URI] =
    mintDplaItemUri(data)

  override def dataProvider(data: Document[JValue]): ZeroToMany[EdmAgent] =
    Seq(nameOnlyAgent("Getty Research Institute"))

  override def isShownAt(data: Document[JValue]): ZeroToMany[EdmWebResource] =
    extractStrings(unwrap(data) \ "delivery" \ "availabilityLinksUrl")
      .map(stringOnlyWebResource)

  override def originalRecord(data: Document[JValue]): ExactlyOne[String] =
    Utils.formatJson(unwrap(data))

  override def preview(data: Document[JValue]): ZeroToMany[EdmWebResource] = {
    // Getty lists every thumbnail twice in delivery.link -- measured across the
    // 2026-09-12 harvest, all 101,384 records carry exactly two thumbnail
    // entries and in every one the two linkURLs are identical. The entries
    // differ only in their blank-node `@id` and a couple of empty attributes,
    // so there is no "preferred" copy to choose between; it is a duplicate, not
    // a choice. Without `distinct` this produces a "more than one value mapped"
    // warning on 100% of Getty records -- 21% of all warnings the hub emits --
    // while Mapper.validatePreview silently keeps whichever came first.
    (unwrap(data) \ "delivery" \ "link")
      .filter(
        extractString("displayLabel")(_)
          .getOrElse("")
          .equalsIgnoreCase("thumbnail")
      )
      .flatMap(extractStrings("linkURL")(_))
      .distinct
      .map(stringOnlyWebResource)
  }

  override def provider(data: Document[JValue]): ExactlyOne[EdmAgent] = agent

  override def sidecar(data: Document[JValue]): JValue =
    ("prehashId", buildProviderBaseId()(data)) ~ ("dplaId", mintDplaId(data))

  // Helper method
  def agent: EdmAgent = EdmAgent(
    name = Some("J. Paul Getty Trust"),
    uri = Some(URI("http://dp.la/api/contributor/getty"))
  )
}
