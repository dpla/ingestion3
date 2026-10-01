/**
 * Provider: Dartmouth Libraries (Dartmouth College). Metadata format: MODS,
 * harvested from the live OAI-PMH feed at
 * https://collections.dartmouth.edu/archive/oai (metadataPrefix=mods). Records
 * arrive OAI-wrapped (<record><metadata><mods:mods>); `getModsRoot` anchors to
 * the record's root MODS element so the mapper also works on a raw <mods:mods>.
 *
 * Mapping decisions reflect Shaun Akhtar's 2026-09-30 email (see
 * docs/ingestion/dartmouth-mapping-draft.md section 4 for the resolution log).
 */
package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.mappers.utils.{Document, XmlExtractor, XmlMapping}
import dpla.ingestion3.model.DplaMapData.{ExactlyOne, ZeroToMany, ZeroToOne}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.xml._

class DartmouthMapping extends XmlMapping with XmlExtractor {

  // Base URL used to resolve relative URLs the feed currently emits (preview).
  private val DartmouthBaseUrl = "https://collections.dartmouth.edu"

  // titleInfo @type values that are NOT the primary title.
  private val alternateTitleTypes: Seq[String] =
    Seq("alternative", "translated", "uniform")

  // Bare FAST authority code, e.g. "(OCoLC)fst01310063" -> id.worldcat.org/fast/1310063
  private val fastCode = """^\(OCoLC\)fst0*([0-9]+)$""".r

  // ID minting functions
  override def useProviderName: Boolean = true

  override def getProviderName: Option[String] = Some("dartmouth")

  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] = {
    // Prefer the item's own DRB record identifier
    // (mods:recordInfo/mods:recordIdentifier[@source="DRB"]); fall back to the OAI
    // header identifier (oai:ddlp-id:<collection>/<item>), then any recordIdentifier.
    val recordIds = getModsRoot(data) \ "recordInfo" \ "recordIdentifier"
    byAttribute(recordIds, "source", "DRB").flatMap(extractStrings).headOption
      .orElse(extractString(data \ "header" \ "identifier"))
      .orElse(extractString(recordIds))
  }

  // ── SourceResource ──────────────────────────────────────────────────────────

  override def alternateTitle(data: Document[NodeSeq]): ZeroToMany[String] = {
    val titleInfos = getModsRoot(data) \ "titleInfo"
    alternateTitleTypes.flatMap(t =>
      byAttribute(titleInfos, "type", t).flatMap(node => extractStrings(node \ "title"))
    )
  }

  override def collection(data: Document[NodeSeq]): Seq[DcmiTypeCollection] =
    // <mods:relatedItem type="host"><mods:titleInfo><mods:title>
    byAttribute(getModsRoot(data) \ "relatedItem", "type", "host")
      .flatMap(c => extractStrings(c \ "titleInfo" \ "title"))
      .map(nameOnlyCollection)

  override def creator(data: Document[NodeSeq]): Seq[EdmAgent] =
    // Per Dartmouth (2026-09-30): names with usage="primary", regardless of type or
    // role. Records with no primary name have no creator.
    (getModsRoot(data) \ "name")
      .filter(n => filterAttribute(n, "usage", "primary"))
      .map(edmAgentHelper)

  override def contributor(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    // Per Dartmouth (2026-09-30): all non-primary names EXCEPT those with the
    // "repository" role (duplicative of provider). Role matched case-insensitively
    // against both roleTerm[@type="text"] ("repository") and [@type="code"] ("rps").
    (getModsRoot(data) \ "name")
      .filterNot(n => filterAttribute(n, "usage", "primary"))
      .filterNot(isRepositoryName)
      .map(edmAgentHelper)

  override def date(data: Document[NodeSeq]): Seq[EdmTimeSpan] = {
    // Per Dartmouth (2026-09-30): the item's original date lives in
    // relatedItem[@type="original" | "otherFormat"]/originInfo/{dateCreated,dateIssued}
    // (@encoding="w3cdtf"); the top-level originInfo date is the digitization date.
    // Prefer the related-item date; fall back to the top-level date.
    val root = getModsRoot(data)
    val relItems = root \ "relatedItem"
    val relOrigin =
      (byAttribute(relItems, "type", "original") ++
        byAttribute(relItems, "type", "otherFormat")) \ "originInfo"

    def w3cdtf(nodes: NodeSeq): Seq[String] =
      byAttribute(nodes, "encoding", "w3cdtf")
        .flatMap(extractStrings)
        .map(_.trim)
        .filter(_.nonEmpty)

    val fromRelated = w3cdtf(relOrigin \ "dateCreated") ++ w3cdtf(relOrigin \ "dateIssued")
    val topOrigin = root \ "originInfo"
    val fromTop = Seq("dateCreated", "dateIssued", "dateOther", "copyrightDate")
      .flatMap(prop => w3cdtf(topOrigin \ prop))

    val chosen = if (fromRelated.nonEmpty) fromRelated else fromTop
    chosen.distinct.map(stringOnlyTimeSpan)
  }

  override def description(data: Document[NodeSeq]): Seq[String] =
    // <mods:abstract> only (direct child), excluding abstract[@shareable="no"]
    // (e.g. "Part 1 of 4"). <mods:note> values are intentionally not mapped.
    (getModsRoot(data) \ "abstract")
      .filterNot(n => filterAttribute(n, "shareable", "no"))
      .flatMap(extractStrings)

  override def extent(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(getModsRoot(data) \ "physicalDescription" \ "extent")

  override def genre(data: Document[NodeSeq]): ZeroToMany[SkosConcept] = {
    // Per Dartmouth (2026-09-30): map MODS genre to DPLA genre, preserving the
    // @valueURI as exactMatch (http as-is; bare FAST codes converted to
    // id.worldcat.org/fast URIs). Deduped by label, keeping a URI-bearing variant.
    val concepts = (getModsRoot(data) \ "genre")
      .map(skosConceptHelper)
      .filter(_.providedLabel.exists(_.trim.nonEmpty))
    val byLabel = scala.collection.mutable.LinkedHashMap[String, SkosConcept]()
    concepts.foreach { c =>
      val key = c.providedLabel.map(_.trim.toLowerCase).getOrElse("")
      val keep = byLabel.get(key).forall(e => e.exactMatch.isEmpty && c.exactMatch.nonEmpty)
      if (keep) byLabel(key) = c
    }
    byLabel.values.toSeq
  }

  override def language(data: Document[NodeSeq]): Seq[SkosConcept] =
    byAttribute(getModsRoot(data) \ "language" \ "languageTerm", "type", "text")
      .flatMap(extractStrings)
      .map(nameOnlyConcept)

  override def place(data: Document[NodeSeq]): Seq[DplaPlace] = {
    val root = getModsRoot(data)

    // <mods:originInfo><mods:place><mods:placeTerm type="text"> — plain-text place.
    val originPlaces =
      byAttribute(root \ "originInfo" \ "place" \ "placeTerm", "type", "text")
        .flatMap(extractStrings)
        .map(_.trim)
        .filter(_.nonEmpty)
        .map(nameOnlyPlace)

    val subjects = root \ "subject"

    // <mods:subject><mods:geographic>, with a FAST/http valueURI as exactMatch.
    val geoPlaces = subjects.flatMap { s =>
      val uri = (getAttributeValue(s, "valueURI").toSeq ++
        (s \ "geographic").flatMap(g => getAttributeValue(g, "valueURI")))
        .flatMap(normalizeAuthorityUri)
      (s \ "geographic")
        .flatMap(extractStrings)
        .map(_.trim)
        .filter(_.nonEmpty)
        .map(name => DplaPlace(name = Some(name), exactMatch = uri))
    }

    // <mods:subject><mods:cartographics><mods:coordinates>. NOTE: Dartmouth emits
    // MARC-255 coordinate strings (e.g. "(W 73°--W 70°/N 45°...)"), not decimal
    // lat/long. Mapped as-is; see the mapping doc / QA report re: MAP 3.1.
    val coordPlaces = (subjects \ "cartographics" \ "coordinates")
      .flatMap(extractStrings)
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(c => DplaPlace(coordinates = Some(c)))

    // <mods:subject><mods:hierarchicalGeographic>
    val hierPlaces = (subjects \ "hierarchicalGeographic").map { h =>
      val city = extractString(h \ "city").map(_.trim).filter(_.nonEmpty)
      val county = extractString(h \ "county").map(_.trim).filter(_.nonEmpty)
      val state = extractString(h \ "state").map(_.trim).filter(_.nonEmpty)
      val country = extractString(h \ "country").map(_.trim).filter(_.nonEmpty)
      val name = Seq(city, county, state, country).flatten match {
        case Nil => None
        case parts => Some(parts.mkString(", "))
      }
      DplaPlace(name = name, city = city, county = county, state = state, country = country)
    }

    originPlaces ++ geoPlaces ++ coordPlaces ++ hierPlaces
  }

  override def publisher(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractStrings(getModsRoot(data) \ "originInfo" \ "publisher")
      .map(nameOnlyAgent)

  override def rights(data: Document[NodeSeq]): Seq[String] =
    // Direct text of each <mods:accessCondition>; a condition whose content is a
    // nested copyrightMD block (cmd:copyright) contributes no text (holder ->
    // rightsHolder). Standardized rights URIs are mapped to edmRights.
    (getModsRoot(data) \ "accessCondition")
      .map(directText)
      .filter(_.nonEmpty)

  override def rightsHolder(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    (getModsRoot(data) \ "accessCondition" \ "copyright" \ "rights.holder" \ "name")
      .flatMap(extractStrings)
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(nameOnlyAgent)

  override def subject(data: Document[NodeSeq]): Seq[SkosConcept] = {
    val root = getModsRoot(data)
    Seq("topic", "temporal", "titleInfo", "name", "genre").flatMap(property =>
      (root \ "subject" \ property).map(skosConceptHelper)
    )
  }

  override def temporal(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
    extractStrings(getModsRoot(data) \ "subject" \ "temporal")
      .map(stringOnlyTimeSpan)

  override def title(data: Document[NodeSeq]): Seq[String] = {
    val titleNodes = (getModsRoot(data) \ "titleInfo")
      .filterNot(n => alternateTitleTypes.exists(t => filterAttribute(n, "type", t)))

    titleNodes
      .map(n => {
        val nonSort = extractStrings(n \ "nonSort").mkString(" ")
        val title = extractStrings(n \ "title").mkString(" ")
        val subTitle = extractStrings(n \ "subTitle").mkString(" ")
        s"$nonSort $title $subTitle".reduceWhitespace
      })
      .filter(_.nonEmpty)
  }

  override def `type`(data: Document[NodeSeq]): Seq[String] =
    extractStrings(getModsRoot(data) \ "typeOfResource")

  // ── OreAggregation ──────────────────────────────────────────────────────────

  override def dplaUri(data: Document[NodeSeq]): ZeroToOne[URI] =
    mintDplaItemUri(data)

  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    // Hardcoded, same as provider. Dartmouth offered to supply a per-record
    // dataProvider source; proposal is in the QA/result file.
    Seq(nameOnlyAgent("Dartmouth Libraries"))

  override def edmRights(data: Document[NodeSeq]): ZeroToMany[URI] =
    // Standardized rights URI: <mods:accessCondition type="use and reproduction"
    // xlink:href="..."> (rightsstatements.org or creativecommons.org).
    byAttribute(getModsRoot(data) \ "accessCondition", "type", "use and reproduction")
      .flatMap(node => node.attribute(node.getNamespace("xlink"), "href"))
      .flatMap(n => extractString(n.head))
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(normalizeRightsScheme)
      .map(URI)

  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] = {
    // Per Dartmouth (2026-09-30): prefer identifier[@type="doi"], then
    // identifier[@type="ark"], then location/url[@usage="primary"][@access="object
    // in context"]. Bare doi:/ark: forms normalized to resolvable URLs.
    val root = getModsRoot(data)
    val doi = byAttribute(root \ "identifier", "type", "doi").flatMap(extractStrings)
    val ark = byAttribute(root \ "identifier", "type", "ark").flatMap(extractStrings)
    val primary = byAttribute(
      byAttribute(root \ "location" \ "url", "usage", "primary"),
      "access", "object in context"
    ).flatMap(extractStrings)

    (doi ++ ark ++ primary)
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(normalizeIsShownAt)
      .headOption
      .map(stringOnlyWebResource)
      .toSeq
  }

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] =
    Utils.formatXml(data)

  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    // <mods:location><mods:url access="preview">. Dartmouth currently emits a
    // RELATIVE path; resolve it against the base URL. (Delete resolveUrl once
    // Dartmouth makes these absolute — see the mapping doc.)
    byAttribute(getModsRoot(data) \ "location" \ "url", "access", "preview")
      .flatMap(extractStrings)
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(resolveUrl)
      .map(stringOnlyWebResource)

  override def iiifManifest(data: Document[NodeSeq]): ZeroToMany[URI] =
    // <mods:location><mods:url note="IIIF manifest"> — literal attribute value
    // "IIIF manifest" (with a space), as the feed emits it.
    byAttribute(getModsRoot(data) \ "location" \ "url", "note", "IIIF manifest")
      .flatMap(extractStrings)
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(resolveUrl)
      .map(URI)

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = agent

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(data))

  // ── Helpers ─────────────────────────────────────────────────────────────────

  def agent: EdmAgent = EdmAgent(
    name = Some("Dartmouth Libraries"),
    uri = Some(URI("http://dp.la/api/contributor/dartmouth"))
  )

  // Anchors to the record's root MODS element (OAI-wrapped <metadata><mods> or a
  // raw <mods>), without matching any nested <mods> inside a relatedItem.
  private def getModsRoot(data: Document[NodeSeq]): NodeSeq = {
    val ns: NodeSeq = data
    val wrapped = ns \ "metadata" \ "mods"
    if (wrapped.nonEmpty) wrapped else ns.filter(_.label == "mods")
  }

  private def byAttribute(nodes: NodeSeq, attr: String, value: String): NodeSeq =
    nodes.flatMap(n => getByAttribute(n.asInstanceOf[Elem], attr, value))

  // A name is the "repository" (excluded from contributor) if any of its role
  // terms is "repository" (text) or "rps" (MARC relator code), case-insensitive.
  private def isRepositoryName(node: Node): Boolean =
    (node \ "role" \ "roleTerm")
      .flatMap(extractStrings)
      .map(_.trim.toLowerCase)
      .exists(t => t == "repository" || t == "rps")

  private def nameConstructor(node: Node): Option[String] = {
    val family = extractString((node \ "namePart").filter(p => filterAttribute(p, "type", "family")))
    val given = extractString((node \ "namePart").filter(p => filterAttribute(p, "type", "given")))
    val date = extractString((node \ "namePart").filter(p => filterAttribute(p, "type", "date")))
    val plain = extractStrings((node \ "namePart").filter(p => p.attributes.isEmpty)).mkString(", ")

    val base = (family, given) match {
      case (Some(f), Some(g)) => Some(s"$f, $g")
      case (Some(f), None)    => Some(f)
      case (None, Some(g))    => Some(g)
      case (None, None)       => if (plain.nonEmpty) Some(plain) else None
    }

    base.map(b => (Seq(b) ++ date).mkString(", "))
  }

  private def edmAgentHelper(node: Node): EdmAgent = {
    // @valueURI -> exactMatch (entity URI); @authorityURI -> scheme. http(s) only.
    val uri = getAttributeValue(node, "valueURI").filter(isHttpUri).map(URI).toSeq
    val scheme = getAttributeValue(node, "authorityURI").filter(isHttpUri).map(URI)
    EdmAgent(name = nameConstructor(node), exactMatch = uri, scheme = scheme)
  }

  private def skosConceptHelper(node: Node): SkosConcept = {
    val uri = getAttributeValue(node, "valueURI").flatMap(normalizeAuthorityUri).toSeq
    val scheme = getAttributeValue(node, "authorityURI").filter(isHttpUri).map(URI)
    SkosConcept(providedLabel = extractString(node), exactMatch = uri, scheme = scheme)
  }

  // http(s) kept as-is; bare FAST "(OCoLC)fst…" codes converted to a FAST URI;
  // anything else dropped (not a resolvable URI).
  private def normalizeAuthorityUri(value: String): Option[URI] = value.trim match {
    case v if isHttpUri(v)   => Some(URI(v))
    case fastCode(n)         => Some(URI(s"http://id.worldcat.org/fast/$n"))
    case _                   => None
  }

  // DPLA's edmRights vocabulary (validEdmRightsValues) and the edmRights
  // enrichment both use the http:// forms of the rights vocabs. A partner that
  // publishes the https:// form of the same statement would otherwise fail the
  // exact-match validation and have its edmRights dropped, so canonicalize the
  // scheme to http (the statement itself is unchanged).
  private def normalizeRightsScheme(value: String): String =
    value.replaceFirst(
      "^https://(creativecommons\\.org|rightsstatements\\.org)/",
      "http://$1/"
    )

  // Normalize an isShownAt candidate to a resolvable URL.
  private def normalizeIsShownAt(value: String): String = value.trim match {
    case s if isHttpUri(s)        => s
    case s if s.startsWith("doi:") => "https://doi.org/" + s.stripPrefix("doi:")
    case s if s.startsWith("ark:") => "https://n2t.net/" + s
    case s                         => s
  }

  // Resolve a possibly-relative URL against the Dartmouth base URL. Dartmouth
  // currently emits relative preview paths; they intend to make them absolute.
  // DELETE this once all URLs in the feed are absolute.
  private def resolveUrl(value: String): String = {
    val t = value.trim
    if (isHttpUri(t)) t
    else DartmouthBaseUrl + (if (t.startsWith("/")) t else "/" + t)
  }

  private def isHttpUri(value: String): Boolean =
    value.startsWith("http://") || value.startsWith("https://")

  // Concatenates only a node's direct text children (ignoring nested elements such
  // as copyrightMD blocks), with whitespace collapsed.
  private def directText(node: Node): String =
    node.child.collect { case t: Text => t.text }.mkString(" ").reduceWhitespace
}
