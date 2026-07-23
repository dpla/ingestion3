/**
 * TEST HUB — NOT APPROVED FOR PRODUCTION
 *
 * This mapper is under evaluation and has not been approved for inclusion
 * in the DPLA production index. Do not remove the `status = test` flag
 * from i3.conf until the hub has been formally approved.
 *
 * See docs/ingestion/README_TEST_HUBS.md for full conventions.
 *
 * Provider: American Archive of Public Broadcasting (AAPB) — a collaboration of
 * WGBH and the Library of Congress. Metadata format: PBCore 2.x
 * (Public Broadcasting Metadata Dictionary), an XML standard derived from
 * Dublin Core. Records are <pbcoreDescriptionDocument> documents (one asset per
 * document). The `pbcoreRoot` helper below anchors to the description document
 * so this mapper works whether records arrive raw (file harvest), wrapped in an
 * OAI <record><metadata> envelope, or inside a <pbcoreCollection>.
 */
package dpla.ingestion3.mappers.providers.experimental

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.mappers.utils.{Document, XmlExtractor, XmlMapping}
import dpla.ingestion3.model.DplaMapData.{ExactlyOne, ZeroToMany, ZeroToOne}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.xml._

class AapbMapping extends XmlMapping with XmlExtractor {

  // The pbcoreIdentifier @source that carries the canonical AAPB (AACIP) id,
  // e.g. "cpb-aacip/508-sf2m61cj57". This id also drives the landing-page URL.
  private val AacipSource = "http://americanarchiveinventory.org"

  // pbcoreTitle @titleType values treated as the primary item title.
  private val primaryTitleTypes = Seq("Title", "Program", "Preferred Title")

  // ID minting functions
  override def useProviderName: Boolean = true

  override def getProviderName: Option[String] = Some("aapb")

  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] = {
    val ids = pbcoreRoot(data) \ "pbcoreIdentifier"
    byAttribute(ids, "source", AacipSource).flatMap(extractStrings).headOption
      .orElse(extractString(ids))
      .orElse(extractString(data \ "header" \ "identifier"))
  }

  // SourceResource mapping

  override def alternateTitle(data: Document[NodeSeq]): ZeroToMany[String] =
    (pbcoreRoot(data) \ "pbcoreTitle")
      .filter(n => getAttributeValue(n, "titleType").exists(_.toLowerCase.contains("alternate")))
      .flatMap(extractStrings)
      .map(_.reduceWhitespace)
      .filter(_.nonEmpty)

  override def collection(data: Document[NodeSeq]): ZeroToMany[DcmiTypeCollection] =
    // The PBCore "Series" title is the closest analog to a DPLA collection.
    // TODO: confirm with AAPB whether Series or the "special_collections"
    //  annotation (a slug, e.g. "vision-maker-media") is the better collection.
    titlesOfType(data, "Series").map(nameOnlyCollection)

  override def contributor(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    (pbcoreRoot(data) \ "pbcoreContributor" \ "contributor").map(edmAgentHelper)

  override def creator(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    (pbcoreRoot(data) \ "pbcoreCreator" \ "creator").map(edmAgentHelper)

  override def date(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
    // <pbcoreAssetDate> of any @dateType. AAPB frequently pads unknown month/day
    // as "00" (e.g. "2002-00-00"); cleanDate strips those to a valid partial date.
    extractStrings(pbcoreRoot(data) \ "pbcoreAssetDate")
      .map(cleanDate)
      .filter(_.nonEmpty)
      .distinct
      .map(stringOnlyTimeSpan)

  override def description(data: Document[NodeSeq]): ZeroToMany[String] =
    // All <pbcoreDescription> values (of every @descriptionType).
    extractStrings(pbcoreRoot(data) \ "pbcoreDescription")
      .map(_.reduceWhitespace)
      .filter(_.nonEmpty)

  override def extent(data: Document[NodeSeq]): ZeroToMany[String] =
    // Duration of the instantiations (e.g. "0:56:46").
    extractStrings(pbcoreRoot(data) \ "pbcoreInstantiation" \ "instantiationDuration")
      .map(_.trim)
      .filter(_.nonEmpty)
      .distinct

  override def format(data: Document[NodeSeq]): ZeroToMany[String] =
    // <pbcoreAssetType> (e.g. "Program", "Episode", "Clip").
    extractStrings(pbcoreRoot(data) \ "pbcoreAssetType")
      .map(_.trim)
      .filter(_.nonEmpty)
      .distinct

  override def genre(data: Document[NodeSeq]): ZeroToMany[SkosConcept] =
    // <pbcoreGenre> that is NOT an AAPB Topical Genre (those go to subject).
    // Captures format/LCGFT genres; @ref (http) -> exactMatch.
    (pbcoreRoot(data) \ "pbcoreGenre")
      .filterNot(isTopicalGenre)
      .map(skosConceptUriHelper)
      .filter(_.providedLabel.exists(_.nonEmpty))

  override def identifier(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(pbcoreRoot(data) \ "pbcoreIdentifier")
      .map(_.trim)
      .filter(_.nonEmpty)

  override def language(data: Document[NodeSeq]): ZeroToMany[SkosConcept] = {
    val root = pbcoreRoot(data)
    (extractStrings(root \ "pbcoreInstantiation" \ "instantiationLanguage") ++
      extractStrings(root \ "pbcoreInstantiation" \\ "essenceTrackLanguage"))
      .map(_.trim)
      .filter(_.nonEmpty)
      .distinct
      .map(nameOnlyConcept)
  }

  override def place(data: Document[NodeSeq]): ZeroToMany[DplaPlace] =
    // <pbcoreCoverage> whose sibling <coverageType> is "Spatial".
    // <coverage>/@ref (http) -> exactMatch (e.g. a Wikidata URI).
    coverageOfType(data, "Spatial").map { c =>
      DplaPlace(name = extractString(c).map(_.reduceWhitespace), exactMatch = refExactMatch(c))
    }

  override def publisher(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    (pbcoreRoot(data) \ "pbcorePublisher" \ "publisher").map(edmAgentHelper)

  override def rights(data: Document[NodeSeq]): ZeroToMany[String] =
    // <pbcoreRightsSummary><rightsSummary> free text. See §4 of the mapping doc:
    // some AAPB records carry no rights statement, which fails DPLA's required
    // `rights`/`edmRights` validation — an open question for the partner.
    extractStrings(pbcoreRoot(data) \ "pbcoreRightsSummary" \ "rightsSummary")
      .map(_.reduceWhitespace)
      .filter(_.nonEmpty)

  override def subject(data: Document[NodeSeq]): ZeroToMany[SkosConcept] = {
    val root = pbcoreRoot(data)
    // AAPB frequently packs several subjects into one ";"-delimited element
    // (e.g. "University of Houston; Football; Astrodome"); split those. A subject
    // carrying an http @ref is kept whole (the URI applies to the whole term).
    val subjects = (root \ "pbcoreSubject").flatMap(splitSubject)
    // AAPB Topical Genre is a topic, not a format genre -> map to subject.
    val topical = (root \ "pbcoreGenre").filter(isTopicalGenre).map(skosConceptUriHelper)
    (subjects ++ topical).filter(_.providedLabel.exists(_.nonEmpty))
  }

  override def temporal(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
    coverageOfType(data, "Temporal")
      .flatMap(extractStrings)
      .map(_.reduceWhitespace)
      .filter(_.nonEmpty)
      .map(stringOnlyTimeSpan)

  override def title(data: Document[NodeSeq]): ZeroToMany[String] = {
    // Prefer an explicit primary title (titleType Title/Program/Preferred Title).
    // Otherwise AAPB records carry the title across Series + Episode parts; join
    // them "Series; Episode" (dropping "Episode Number" and other qualifiers).
    val primary = primaryTitleTypes.flatMap(t => titlesOfType(data, t)).distinct
    val composite = titlesOfType(data, "Series") ++ titlesOfType(data, "Episode")
    if (primary.nonEmpty) primary
    else if (composite.nonEmpty) Seq(composite.mkString("; "))
    else extractStrings(pbcoreRoot(data) \ "pbcoreTitle").map(_.reduceWhitespace).filter(_.nonEmpty)
  }

  override def `type`(data: Document[NodeSeq]): ZeroToMany[String] =
    // DCMI-ish type from the instantiation media type (e.g. "Moving Image",
    // "Sound"). Type enrichment normalizes these to DCMI values.
    extractStrings(pbcoreRoot(data) \ "pbcoreInstantiation" \ "instantiationMediaType")
      .map(_.trim)
      .filter(_.nonEmpty)
      .distinct

  // OreAggregation

  override def dplaUri(data: Document[NodeSeq]): ZeroToOne[URI] =
    mintDplaItemUri(data)

  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] = {
    // The contributing/holding organization is recorded as a top-level
    // <pbcoreAnnotation annotationType="organization">. Fall back to AAPB.
    // TODO: confirm the dataProvider convention with AAPB.
    val orgs = (pbcoreRoot(data) \ "pbcoreAnnotation")
      .filter(n => filterAttribute(n, "annotationType", "organization"))
      .flatMap(extractStrings)
      .map(_.trim)
      .filter(_.nonEmpty)
      .distinct
    if (orgs.nonEmpty) orgs.map(nameOnlyAgent)
    else Seq(nameOnlyAgent("American Archive of Public Broadcasting"))
  }

  override def edmRights(data: Document[NodeSeq]): ZeroToMany[URI] =
    // <pbcoreRightsSummary><rightsLink> holds a standardized rights URI when present.
    extractStrings(pbcoreRoot(data) \ "pbcoreRightsSummary" \ "rightsLink")
      .map(_.trim)
      .filter(isHttpUri)
      .map(URI)

  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    // Landing page in the AAPB catalog, keyed by the underscore form of the
    // AACIP id (the id arrives as "cpb-aacip/NNN-xxxx" or "cpb-aacip-NNN-xxxx"
    // and becomes "cpb-aacip_NNN-xxxx" in the catalog URL).
    aacipGuid(data)
      .map(id => s"https://americanarchive.org/catalog/${catalogForm(id)}")
      .map(stringOnlyWebResource)
      .toSeq

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] =
    Utils.formatXml(data)

  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] = {
    // AAPB serves a per-asset thumbnail on S3, keyed by the all-hyphen form of
    // the AACIP id. Only "Online Reading Room" (ORR) assets have a real thumbnail;
    // non-ORR (on-location) assets resolve to a "*_NOT_AVAIL.png" placeholder, so
    // we only emit a preview when the record is flagged ORR.
    val root = pbcoreRoot(data)
    if (isOnlineReadingRoom(root))
      aacipGuid(data)
        .map(id => s"https://s3.amazonaws.com/americanarchive.org/thumbnail/${hyphenForm(id)}.jpg")
        .map(stringOnlyWebResource)
        .toSeq
    else Seq()
  }

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = agent

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(data))

  // Helper methods

  def agent: EdmAgent = EdmAgent(
    name = Some("American Archive of Public Broadcasting"),
    uri = Some(URI("http://dp.la/api/contributor/aapb"))
  )

  // Anchors to the <pbcoreDescriptionDocument>, whether it is the top-level node
  // (raw file harvest), nested in an OAI <metadata> wrapper, or inside a
  // <pbcoreCollection>. `\\` is descendant-or-self, so a raw root matches itself.
  private def pbcoreRoot(data: Document[NodeSeq]): NodeSeq = {
    val ns: NodeSeq = data
    val doc = ns \\ "pbcoreDescriptionDocument"
    if (doc.nonEmpty) doc else ns
  }

  // The canonical AAPB (AACIP) identifier, e.g. "cpb-aacip/508-sf2m61cj57".
  private def aacipGuid(data: Document[NodeSeq]): Option[String] =
    byAttribute(pbcoreRoot(data) \ "pbcoreIdentifier", "source", AacipSource)
      .flatMap(extractStrings)
      .map(_.trim)
      .find(_.nonEmpty)

  // The AACIP id arrives with the separator after "cpb-aacip" as "/", "-", or "_".
  // The catalog/landing URL uses the "_" form; thumbnail/media URLs use the "-" form.
  private def catalogForm(id: String): String = id.replaceFirst("^cpb-aacip[/_-]", "cpb-aacip_")

  private def hyphenForm(id: String): String = id.replaceFirst("^cpb-aacip[/_-]", "cpb-aacip-")

  // A record is viewable online when flagged "Online Reading Room".
  private def isOnlineReadingRoom(root: NodeSeq): Boolean =
    (root \ "pbcoreAnnotation")
      .filter(n => filterAttribute(n, "annotationType", "Level of User Access"))
      .flatMap(extractStrings)
      .exists(_.equalsIgnoreCase("Online Reading Room"))

  private def splitSubject(node: Node): Seq[SkosConcept] =
    // A subject with an http @ref is kept whole (the URI applies to the whole
    // term); otherwise split ";"-delimited text into separate concepts.
    if (getAttributeValue(node, "ref").exists(isHttpUri)) Seq(skosConceptUriHelper(node))
    else
      extractStrings(node)
        .flatMap(_.splitAtDelimiter(";"))
        .map(_.reduceWhitespace)
        .filter(_.nonEmpty)
        .map(nameOnlyConcept)

  private def titlesOfType(data: Document[NodeSeq], titleType: String): Seq[String] =
    byAttribute(pbcoreRoot(data) \ "pbcoreTitle", "titleType", titleType)
      .flatMap(extractStrings)
      .map(_.reduceWhitespace)
      .filter(_.nonEmpty)

  // <pbcoreCoverage> whose child <coverageType> equals `coverageType`; returns the
  // <coverage> element(s).
  private def coverageOfType(data: Document[NodeSeq], coverageType: String): NodeSeq =
    (pbcoreRoot(data) \ "pbcoreCoverage").flatMap { cov =>
      if (extractStrings(cov \ "coverageType").exists(_.equalsIgnoreCase(coverageType)))
        cov \ "coverage"
      else NodeSeq.Empty
    }

  private def isTopicalGenre(node: Node): Boolean =
    getAttributeValue(node, "source").exists(_.equalsIgnoreCase("AAPB Topical Genre")) ||
      getAttributeValue(node, "annotation").exists(_.equalsIgnoreCase("topic"))

  // @ref -> exactMatch (an authority/CV URI, e.g. an LC name authority or a
  // Wikidata term); http(s) only.
  private def refExactMatch(node: Node): Seq[URI] =
    getAttributeValue(node, "ref").filter(isHttpUri).map(URI).toSeq

  private def edmAgentHelper(node: Node): EdmAgent =
    EdmAgent(name = extractString(node).map(_.reduceWhitespace), exactMatch = refExactMatch(node))

  private def skosConceptUriHelper(node: Node): SkosConcept =
    SkosConcept(providedLabel = extractString(node).map(_.reduceWhitespace), exactMatch = refExactMatch(node))

  // Strips PBCore "00" month/day padding: "2002-00-00" -> "2002", "1959-10-00" ->
  // "1959-10". Leaves fully-specified dates untouched.
  private def cleanDate(value: String): String = {
    val trimmed = value.trim
    val datePart = trimmed.split("[T ]").headOption.getOrElse(trimmed)
    datePart.split("-").takeWhile(_ != "00").mkString("-")
  }

  private def isHttpUri(value: String): Boolean =
    value.startsWith("http://") || value.startsWith("https://")

  private def byAttribute(nodes: NodeSeq, attr: String, value: String): NodeSeq =
    nodes.flatMap(n => getByAttribute(n.asInstanceOf[Elem], attr, value))
}
