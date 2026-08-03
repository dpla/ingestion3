/** TEST HUB — NOT APPROVED FOR PRODUCTION
  *
  * This mapper is under evaluation and has not been approved for inclusion in
  * the DPLA production index. Do not remove the `status = test` flag from
  * i3.conf until the hub has been formally approved.
  *
  * See docs/ingestion/README_TEST_HUBS.md and
  * docs/ingestion/amp-mapping-draft.md for conventions and the draft mapping.
  */
package dpla.ingestion3.mappers.providers.experimental

import dpla.ingestion3.mappers.utils._
import dpla.ingestion3.model.DplaMapData.{
  AtLeastOne,
  ExactlyOne,
  ZeroToMany,
  ZeroToOne
}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.json4s._
import org.json4s.JsonDSL._

/** Arizona Memory Project (Arizona Memory Portal) — Recollect platform.
  *
  * Input: one JSON object per item as produced by the harvester (and mirrored
  * by src/test/resources/arizona/sample-records.json), shaped:
  * {{{
  *   {
  *     "id": "23308",
  *     "isShownAt": "https://azmemory.azlibrary.gov/nodes/view/23308",
  *     "title": "...",
  *     "preview": "https://azmemory.azlibrary.gov/assets/pic/23308",
  *     "rights": { "iconCode": "9", "label": null,
  *                 "uri": "http://rightsstatements.org/vocab/NoC-US/1.0/",
  *                 "statement": "..." },
  *     "fields": { "Creator": ["..."], "Subject": ["...", "..."], ... }
  *   }
  * }}}
  * `fields` keys are the AMP metadata labels verbatim.
  */
class ArizonaMapping extends JsonMapping with JsonExtractor {

  // Read a repeatable AMP metadata field (fields.<label>) as a Seq[String].
  private def field(name: String)(data: Document[JValue]): Seq[String] =
    extractStrings(unwrap(data) \ "fields" \ name)

  private def firstField(name: String)(data: Document[JValue]): Option[String] =
    field(name)(data).headOption

  // ID minting — AMP ids are the bare Recollect node id, so salt with "arizona".
  override def useProviderName: Boolean = true

  override def getProviderName: Option[String] = Some("arizona")

  override def originalId(implicit data: Document[JValue]): ZeroToOne[String] =
    extractString(unwrap(data) \ "id")

  // OreAggregation

  override def dplaUri(data: Document[JValue]): ZeroToOne[URI] =
    mintDplaItemUri(data)

  override def provider(data: Document[JValue]): ExactlyOne[EdmAgent] = agent

  override def dataProvider(data: Document[JValue]): ZeroToMany[EdmAgent] =
    field("Contributing Institution")(data).map(nameOnlyAgent)

  override def isShownAt(data: Document[JValue]): ZeroToMany[EdmWebResource] =
    extractString(unwrap(data) \ "isShownAt")
      .orElse(
        originalId(data).map(id =>
          s"https://azmemory.azlibrary.gov/nodes/view/$id"
        )
      )
      .map(stringOnlyWebResource)
      .toSeq

  // og:image falls back to a site-chrome asset under /theme/ when an item has
  // no thumbnail (e.g. logo.mobile.png); don't emit those placeholders. All
  // Recollect theme assets live under /theme/, so the path scope is sufficient.
  override def preview(data: Document[JValue]): ZeroToMany[EdmWebResource] =
    extractString(unwrap(data) \ "preview")
      .filterNot(_.contains("/theme/"))
      .map(stringOnlyWebResource)
      .toSeq

  // edmRights: ONLY the partner-published rights URI. Non-standard AMP rights
  // categories carry no URI and are intentionally left to free-text `rights`
  // (see docs/ingestion/amp-mapping-draft.md §3.1 — no invented crosswalk).
  override def edmRights(data: Document[JValue]): ZeroToMany[URI] =
    extractString(unwrap(data) \ "rights" \ "uri")
      .filter(_.startsWith("http"))
      .map(URI)
      .toSeq

  override def originalRecord(data: Document[JValue]): ExactlyOne[String] =
    Utils.formatJson(data)

  override def sidecar(data: Document[JValue]): JValue =
    ("prehashId", buildProviderBaseId()(data)) ~ ("dplaId", mintDplaId(data))

  // SourceResource

  override def title(data: Document[JValue]): AtLeastOne[String] =
    extractString(unwrap(data) \ "title").toSeq

  override def alternateTitle(data: Document[JValue]): ZeroToMany[String] =
    field("Alternate Title")(data)

  override def creator(data: Document[JValue]): ZeroToMany[EdmAgent] =
    field("Creator")(data).map(nameOnlyAgent)

  override def contributor(data: Document[JValue]): ZeroToMany[EdmAgent] =
    field("Contributor")(data).map(nameOnlyAgent)

  override def publisher(data: Document[JValue]): ZeroToMany[EdmAgent] =
    field("Publisher")(data).map(nameOnlyAgent)

  override def description(data: Document[JValue]): ZeroToMany[String] =
    field("Description")(data)

  // Prefer the specific "Date Original"; fall back to the bucketed "Date Range"
  // (e.g. "1940s (1940-1949)"). begin/end are derived downstream by the date
  // enrichment (DateBuilder.generateBeginEnd), so we only set originalSourceDate.
  override def date(data: Document[JValue]): ZeroToMany[EdmTimeSpan] = {
    val original = field("Date Original")(data)
    val dates = if (original.nonEmpty) original else field("Date Range")(data)
    dates.map(stringOnlyTimeSpan)
  }

  override def language(data: Document[JValue]): ZeroToMany[SkosConcept] =
    field("Language")(data).map(nameOnlyConcept)

  override def subject(data: Document[JValue]): ZeroToMany[SkosConcept] =
    (field("Subject")(data) ++ field("Topic")(data)).map(nameOnlyConcept)

  override def `type`(data: Document[JValue]): ZeroToMany[String] =
    field("Type")(data)

  override def format(data: Document[JValue]): ZeroToMany[String] =
    field("Original Format")(data)

  override def collection(data: Document[JValue]): ZeroToMany[DcmiTypeCollection] =
    field("Collection")(data).map(nameOnlyCollection)

  override def identifier(data: Document[JValue]): ZeroToMany[String] =
    field("Identifier")(data) ++
      field("Call Number")(data) ++
      field("OCLC Number")(data) ++
      field("Library of Congress Call Number (LCCN)")(data)

  // Build a hierarchical place from the discrete AMP geography fields, plus any
  // free-standing place-name facets.
  override def place(data: Document[JValue]): ZeroToMany[DplaPlace] = {
    val hierarchical = DplaPlace(
      city = firstField("City or Town")(data),
      county = firstField("County")(data),
      state = firstField("State")(data),
      country = firstField("Country")(data)
    )
    val named =
      (field("Place")(data) ++
        field("Geographic Feature")(data) ++
        field("Tribal Homeland")(data)).map(nameOnlyPlace)
    (if (hierarchical == DplaPlace()) Seq.empty else Seq(hierarchical)) ++ named
  }

  override def rights(data: Document[JValue]): AtLeastOne[String] =
    extractString(unwrap(data) \ "rights" \ "statement").toSeq

  private def agent: EdmAgent = EdmAgent(
    name = Some("Arizona Memory Project"),
    uri = Some(URI("http://dp.la/api/contributor/arizona"))
  )
}
