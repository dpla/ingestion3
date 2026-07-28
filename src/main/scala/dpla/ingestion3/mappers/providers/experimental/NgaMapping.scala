/**
 * TEST HUB — NOT APPROVED FOR PRODUCTION
 *
 * FIRST-PASS DRAFT mapping for the National Gallery of Art Open Data Program.
 *
 * Maps the self-contained JSON document assembled by
 * [[dpla.ingestion3.harvesters.file.NgaFileHarvester]] — an `objects.csv` row
 * plus joined `images`, `constituents` (with the looked-up `constituent`
 * record), `terms`, and `textEntries`. See docs/ingestion/nga-mapping-draft.md.
 *
 * Several field choices are provisional (marked `TODO`) and need review — notably
 * the isShownAt URL pattern, the rights statement, and the DCMI `type` default.
 * Do not remove `nga.status = test` from i3.conf until the hub is approved.
 */
package dpla.ingestion3.mappers.providers.experimental

import dpla.ingestion3.mappers.utils.{Document, JsonExtractor, JsonMapping}
import dpla.ingestion3.model.DplaMapData.{
  AtLeastOne,
  ExactlyOne,
  ZeroToMany,
  ZeroToOne
}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.json4s.JsonDSL._
import org.json4s._

class NgaMapping extends JsonMapping with JsonExtractor {

  private val CC0 = "https://creativecommons.org/publicdomain/zero/1.0/"

  // ID minting
  override def useProviderName: Boolean = true
  override def getProviderName: Option[String] = Some("nga")

  override def originalId(implicit data: Document[JValue]): ZeroToOne[String] =
    extractString(unwrap(data) \ "objectid")

  // -- helpers ---------------------------------------------------------------

  private def objectId(data: Document[JValue]): Option[String] =
    extractString(unwrap(data) \ "objectid").filter(_.nonEmpty)

  private def images(data: Document[JValue]): Seq[JValue] =
    iterify(unwrap(data) \ "images").children

  private def isOpenAccess(img: JValue): Boolean =
    extractString(img \ "openaccess").contains("1")

  private def primaryImage(data: Document[JValue]): Option[JValue] = {
    val imgs = images(data)
    imgs.find(i => extractString(i \ "viewtype").contains("primary"))
      .orElse(imgs.headOption)
  }

  private def terms(data: Document[JValue]): Seq[JValue] =
    iterify(unwrap(data) \ "terms").children

  private def termsOfType(data: Document[JValue], types: Set[String]): Seq[String] =
    terms(data)
      .filter(t => extractString(t \ "termtype").exists(types.contains))
      .flatMap(t => extractString(t \ "term"))
      .filter(_.nonEmpty)
      .distinct

  /** Getty ULAN + Wikidata authority URIs for a constituent record. */
  private def agentExactMatch(constituent: JValue): ZeroToMany[URI] = {
    val ulan = extractString(constituent \ "ulanid")
      .filter(_.nonEmpty)
      .map(id => URI(s"http://vocab.getty.edu/ulan/$id"))
    val wiki = extractString(constituent \ "wikidataid")
      .filter(_.nonEmpty)
      .map(id => URI(s"http://www.wikidata.org/entity/$id"))
    (ulan ++ wiki).toSeq
  }

  // -- OreAggregation --------------------------------------------------------

  override def dplaUri(data: Document[JValue]): ZeroToOne[URI] =
    mintDplaItemUri(data)

  override def provider(data: Document[JValue]): ExactlyOne[EdmAgent] = agent

  // NGA is a single-institution hub: it is both the provider and the holder.
  override def dataProvider(data: Document[JValue]): ZeroToMany[EdmAgent] =
    Seq(nameOnlyAgent("National Gallery of Art"))

  // The canonical page is /artworks/{id}-{title-slug} (verified in-browser), but
  // the slug is not reliably reconstructable from the data (accents, punctuation,
  // NGA's exact slug rules). The legacy /collection/art-object-page.{id}.html is
  // fully constructible from objectid and 301-redirects to the canonical page,
  // so it is used here. (Bare /artworks/{id} without a slug 404s.)
  override def isShownAt(data: Document[JValue]): ZeroToMany[EdmWebResource] =
    objectId(data)
      .map(id =>
        stringOnlyWebResource(s"https://www.nga.gov/collection/art-object-page.$id.html"))
      .toSeq

  // Thumbnail (served to the API as `object`): the primary image's IIIF thumb.
  override def preview(data: Document[JValue]): ZeroToMany[EdmWebResource] =
    primaryImage(data)
      .flatMap(i => extractString(i \ "iiifthumburl"))
      .filter(_.nonEmpty)
      .map(stringOnlyWebResource)
      .toSeq

  // The fullest image NGA publishes for every image on the object (multivalue —
  // e.g. all plates of a portfolio). Built from the IIIF Image API base; this is
  // whatever resolution the institution serves (open-access images are uncapped;
  // rights-restricted ones are pixel-capped by NGA — we surface the fullest
  // available either way rather than distinguishing here). Not gated on rights.
  override def mediaMaster(data: Document[JValue]): ZeroToMany[EdmWebResource] =
    images(data)
      .flatMap(i => extractString(i \ "iiifurl"))
      .filter(_.nonEmpty)
      .map(u => stringOnlyWebResource(s"$u/full/full/0/default.jpg"))

  // NGA's CSVs carry NO rights field; the only rights signal is the per-image
  // `openaccess` flag (1 = CC0 under NGA's Open Access policy). Open-access
  // records get the CC0 `edmRights` URI — which alone satisfies DPLA's rights
  // requirement (a record is rejected only when BOTH `rights` and `edmRights`
  // are empty; see Mapper.validateRights), and a standardized URI is preferred
  // over free text. Records with no open-access image get neither `rights` nor
  // `edmRights` and are intentionally rejected by that validation — scoping
  // NGA's contribution to its open-access works rather than inventing rights.
  override def edmRights(data: Document[JValue]): ZeroToMany[URI] =
    if (images(data).exists(isOpenAccess)) Seq(URI(CC0)) else Seq()

  override def originalRecord(data: Document[JValue]): ExactlyOne[String] =
    Utils.formatJson(data)

  override def sidecar(data: Document[JValue]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(data))

  // -- SourceResource --------------------------------------------------------

  override def title(data: Document[JValue]): AtLeastOne[String] =
    extractString(unwrap(data) \ "title").filter(_.nonEmpty).toSeq

  // Creators = constituents whose roletype is "artist" (includes "artist after").
  // Donors/owners (provenance) are intentionally not mapped as agents.
  override def creator(data: Document[JValue]): ZeroToMany[EdmAgent] =
    iterify(unwrap(data) \ "constituents").children
      .filter(c => extractString(c \ "roletype").contains("artist"))
      .flatMap { c =>
        val cc = c \ "constituent"
        extractString(cc \ "forwarddisplayname")
          .orElse(extractString(cc \ "preferreddisplayname"))
          .filter(_.nonEmpty)
          .map(n => EdmAgent(name = Some(n), exactMatch = agentExactMatch(cc)))
      }
      .distinct

  override def date(data: Document[JValue]): ZeroToMany[EdmTimeSpan] = {
    val display = extractString(unwrap(data) \ "displaydate").filter(_.nonEmpty)
    val begin = extractString(unwrap(data) \ "beginyear").filter(_.nonEmpty)
    val end = extractString(unwrap(data) \ "endyear").filter(_.nonEmpty)
    if (display.isEmpty && begin.isEmpty && end.isEmpty) Seq()
    else Seq(EdmTimeSpan(begin = begin, end = end, originalSourceDate = display))
  }

  // Description = any `brief_narrative` text entry, plus the primary image's
  // `assistivetext` — NGA's per-image "Visual Description" (the rich text shown
  // under that heading on nga.gov). brief_narrative is usually absent, so the
  // visual description is the main descriptive text. (bibliography / exhibition
  // history text entries are dropped.)
  override def description(data: Document[JValue]): ZeroToMany[String] = {
    val briefNarrative = iterify(unwrap(data) \ "textEntries").children
      .filter(t => extractString(t \ "texttype").contains("brief_narrative"))
      .flatMap(t => extractString(t \ "text"))
    val visualDescription =
      primaryImage(data).flatMap(i => extractString(i \ "assistivetext")).toSeq
    (briefNarrative ++ visualDescription).filter(_.nonEmpty).distinct
  }

  // Keyword/Theme/Style/School/Technique are all topical; "Place Executed" →
  // place, "Systematic Catalogue Volume" (a catalogue reference) is dropped.
  override def subject(data: Document[JValue]): ZeroToMany[SkosConcept] =
    termsOfType(data, Set("Keyword", "Theme", "Style", "School", "Technique"))
      .map(nameOnlyConcept)

  override def place(data: Document[JValue]): ZeroToMany[DplaPlace] =
    termsOfType(data, Set("Place Executed")).map(nameOnlyPlace)

  override def format(data: Document[JValue]): ZeroToMany[String] =
    Seq(
      extractString(unwrap(data) \ "classification"),
      extractString(unwrap(data) \ "medium")
    ).flatten.filter(_.nonEmpty).distinct

  override def extent(data: Document[JValue]): ZeroToMany[String] =
    extractString(unwrap(data) \ "dimensions").filter(_.nonEmpty).toSeq

  override def identifier(data: Document[JValue]): ZeroToMany[String] =
    extractString(unwrap(data) \ "accessionnum").filter(_.nonEmpty).toSeq

  override def collection(data: Document[JValue]): ZeroToMany[DcmiTypeCollection] =
    Seq(
      extractString(unwrap(data) \ "series"),
      extractString(unwrap(data) \ "portfolio"),
      extractString(unwrap(data) \ "volume")
    ).flatten.filter(_.nonEmpty).distinct.map(nameOnlyCollection)

  // DCMI type derived from the object's `classification` (12 distinct values —
  // a clean signal, unlike the free-text `medium`). Everything is a visual work
  // (→ "image") except bound Volumes (→ "text") and Time-Based Media Art
  // (→ "moving image").
  override def `type`(data: Document[JValue]): ZeroToMany[String] =
    extractString(unwrap(data) \ "classification")
      .filter(_.nonEmpty)
      .map(NgaMapping.classificationToType)
      .toSeq

  // `rights` (free text) is intentionally NOT overridden — NGA has no rights
  // text, and CC0 is carried as the preferred `edmRights` URI above.

  def agent: EdmAgent = EdmAgent(
    name = Some("National Gallery of Art"),
    uri = Some(URI("http://dp.la/api/contributor/nga"))
  )
}

object NgaMapping {

  /** NGA `classification` → DCMI type. All 12 NGA classifications are visual
    * works except Volume (text) and Time-Based Media Art (moving image).
    */
  val classificationToType: Map[String, String] =
    Map(
      "Volume" -> "text",
      "Time-Based Media Art" -> "moving image"
    ).withDefaultValue("image")
}
