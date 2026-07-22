/**
 * TEST HUB — NOT APPROVED FOR PRODUCTION
 *
 * This mapper is under evaluation and has not been approved for inclusion
 * in the DPLA production index. Do not remove the `status = test` flag
 * from i3.conf until the hub has been formally approved.
 *
 * See docs/ingestion/README_TEST_HUBS.md for full conventions.
 *
 * Provider: Dartmouth Libraries (Dartmouth College). Metadata format: MODS 3.6
 * (with a Dartmouth `drb:` extension namespace). Records are delivered as raw
 * <mods:mods> documents (one record per file). The `getModsRoot` helper below
 * anchors to the record's root MODS element so this mapper works whether records
 * arrive raw (file harvest) or wrapped in an OAI <record><metadata> envelope —
 * without matching any nested <mods:mods> that may appear inside a relatedItem.
 */
package dpla.ingestion3.mappers.providers.experimental

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.enrichments.normalizations.filters.{
  DigitalSurrogateBlockList,
  FormatTypeValuesBlockList
}
import dpla.ingestion3.mappers.utils.{Document, XmlExtractor, XmlMapping}
import dpla.ingestion3.model.DplaMapData.{ExactlyOne, ZeroToMany, ZeroToOne}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.xml._

class DartmouthMapping extends XmlMapping with XmlExtractor {

  val formatBlockList: Set[String] =
    DigitalSurrogateBlockList.termList ++
      FormatTypeValuesBlockList.termList

  // Roles (mods:name/mods:role/mods:roleTerm) that should be treated as creators.
  // Anything else with a role becomes a contributor; the "repository" role is the
  // dataProvider and is excluded from both. Names with no role default to creator.
  private val creatorRoles: Set[String] = Set(
    "creator", "author", "artist", "photographer", "composer",
    "cartographer", "engraver", "illustrator", "sculptor"
  )

  // titleInfo @type values that are NOT the primary title.
  private val alternateTitleTypes: Seq[String] =
    Seq("alternative", "translated", "uniform")

  // ID minting functions
  override def useProviderName: Boolean = true

  override def getProviderName: Option[String] = Some("dartmouth")

  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] = {
    // Prefer the Dartmouth record identifier (mods:recordInfo/mods:recordIdentifier[@source="DRB"]),
    // e.g. "occom-765122". Fall back to any recordIdentifier, then an OAI header identifier
    // (present only if these are later harvested via OAI).
    val recordIds = getModsRoot(data) \ "recordInfo" \ "recordIdentifier"

    byAttribute(recordIds, "source", "DRB").flatMap(extractStrings).headOption
      .orElse(extractString(recordIds))
      .orElse(extractString(data \ "header" \ "identifier"))
  }

  // SourceResource mapping

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

  override def contributor(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    (getModsRoot(data) \ "name")
      .filter(n => {
        val roles = roleTerms(n)
        roles.nonEmpty &&
        !roles.contains("repository") &&
        !roles.exists(creatorRoles.contains)
      })
      .map(edmAgentHelper)

  override def creator(data: Document[NodeSeq]): Seq[EdmAgent] =
    (getModsRoot(data) \ "name")
      .filter(n => {
        val roles = roleTerms(n)
        !roles.contains("repository") &&
        (roles.isEmpty || roles.exists(creatorRoles.contains))
      })
      .map(edmAgentHelper)

  override def date(data: Document[NodeSeq]): Seq[EdmTimeSpan] = {
    // The meaningful item date is often in the analog original, carried in
    // <mods:relatedItem type="otherFormat"><mods:originInfo><mods:dateCreated>,
    // while the top-level <mods:originInfo><mods:dateIssued> is the digitization
    // date. Prefer dateCreated (top-level or otherFormat); fall back to other
    // top-level date properties. TODO: confirm date precedence with the partner.
    val root = getModsRoot(data)
    val topOrigin = root \ "originInfo"
    val otherFormatOrigin =
      byAttribute(root \ "relatedItem", "type", "otherFormat") \ "originInfo"

    def w3cdtf(nodes: NodeSeq): Seq[String] =
      byAttribute(nodes, "encoding", "w3cdtf")
        .flatMap(extractStrings)
        .map(_.trim)
        .filter(_.nonEmpty)

    val dateCreated =
      w3cdtf(topOrigin \ "dateCreated") ++ w3cdtf(otherFormatOrigin \ "dateCreated")

    val fallback = Seq("dateIssued", "dateOther", "copyrightDate")
      .flatMap(prop => w3cdtf(topOrigin \ prop))

    val chosen = if (dateCreated.nonEmpty) dateCreated else fallback
    chosen.distinct.map(stringOnlyTimeSpan)
  }

  override def description(data: Document[NodeSeq]): Seq[String] =
    // <mods:abstract> only (direct child — relatedItem abstracts excluded), and
    // excluding abstract[@shareable="no"] (e.g. "Part 1 of 4", not descriptive).
    // NOTE: <mods:note> values are potentially mappable to description as well,
    // but the Dartmouth samples mix content notes with administrative/technical
    // ones (TEI conversion, Handwriting, Paper, Ink). Left out for now; revisit
    // with the partner if some note types should be included.
    (getModsRoot(data) \ "abstract")
      .filterNot(n => filterAttribute(n, "shareable", "no"))
      .flatMap(extractStrings)

  override def extent(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(getModsRoot(data) \ "physicalDescription" \ "extent")

  override def format(data: Document[NodeSeq]): Seq[String] =
    // <mods:genre>
    extractStrings(getModsRoot(data) \ "genre")
      .map(_.applyBlockFilter(formatBlockList))
      .filter(_.nonEmpty)

  override def identifier(data: Document[NodeSeq]): Seq[String] =
    extractStrings(getModsRoot(data) \ "identifier")

  override def language(data: Document[NodeSeq]): Seq[SkosConcept] =
    // <mods:language><mods:languageTerm type="text">
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

    // <mods:subject><mods:geographic>. An http(s) valueURI on the subject (or the
    // geographic node) is captured as exactMatch. FAST "(OCoLC)fst..." style values
    // are not URIs and are ignored. TODO: capture FAST authority IDs if the partner
    // provides them (or we convert them) in http form (id.worldcat.org/fast/...).
    val subjectPlaces = (root \ "subject").flatMap { subject =>
      val uri = (getAttributeValue(subject, "valueURI").toSeq ++
        (subject \ "geographic").flatMap(g => getAttributeValue(g, "valueURI")))
        .filter(isHttpUri)
        .map(URI)
      (subject \ "geographic")
        .flatMap(extractStrings)
        .map(_.trim)
        .filter(_.nonEmpty)
        .map(name => DplaPlace(name = Some(name), exactMatch = uri))
    }

    originPlaces ++ subjectPlaces
  }

  override def publisher(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractStrings(getModsRoot(data) \ "originInfo" \ "publisher")
      .map(nameOnlyAgent)

  override def rights(data: Document[NodeSeq]): Seq[String] =
    // Direct text of each <mods:accessCondition> only. Using direct text (not
    // descendant text) drops conditions whose content is a nested copyrightMD
    // block (cmd:copyright); its holder is mapped to rightsHolder instead.
    (getModsRoot(data) \ "accessCondition")
      .map(directText)
      .filter(_.nonEmpty)

  override def rightsHolder(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    // <mods:accessCondition><cmd:copyright><cmd:rights.holder><cmd:name>
    (getModsRoot(data) \ "accessCondition" \ "copyright" \ "rights.holder" \ "name")
      .flatMap(extractStrings)
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(nameOnlyAgent)

  override def subject(data: Document[NodeSeq]): Seq[SkosConcept] = {
    val root = getModsRoot(data)
    Seq("topic", "temporal", "titleInfo", "name", "genre").flatMap(property =>
      (root \ "subject" \ property).map(skosConceptUriHelper)
    )
  }

  override def temporal(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
    extractStrings(getModsRoot(data) \ "subject" \ "temporal")
      .map(stringOnlyTimeSpan)

  override def title(data: Document[NodeSeq]): Seq[String] = {
    // Top-level <mods:titleInfo> that is not an alternate form: combine
    // <mods:nonSort> <mods:title> <mods:subTitle>.
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
    // <mods:typeOfResource> (direct child only)
    extractStrings(getModsRoot(data) \ "typeOfResource")

  // OreAggregation
  override def dplaUri(data: Document[NodeSeq]): ZeroToOne[URI] =
    mintDplaItemUri(data)

  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] = {
    // Preferred: the holding institution from the analog original's
    // <mods:subLocation>, e.g.
    //   "Rauner Special Collections Library, 6065 Webster Hall, Hanover, NH 03755, USA"
    // We take the text before the first comma as the institution name and drop the
    // trailing street address.
    //
    // TODO: ask Dartmouth to subfield the institution name and address separately
    //  in the MODS (distinct elements) so we don't have to split on commas — the
    //  split is brittle if an institution name itself contains a comma.
    //
    // Fall back to the repository name (mods:name role="repository", e.g. "Digital
    // by Dartmouth Library"), then a generic default, for records with no
    // subLocation (e.g. the BCM images set).
    val root = getModsRoot(data)

    val fromSubLocation = (root \ "relatedItem" \\ "subLocation")
      .flatMap(extractStrings)
      .map(_.split(",").head.trim)
      .filter(_.nonEmpty)

    val fromRepository = (root \ "name")
      .filter(n => roleTerms(n).contains("repository"))
      .flatMap(nameConstructor)

    val name = fromSubLocation.headOption
      .orElse(fromRepository.headOption)
      .getOrElse("Dartmouth College Library")

    Seq(nameOnlyAgent(name))
  }

  override def edmRights(data: Document[NodeSeq]): ZeroToMany[URI] =
    // <mods:accessCondition type="use and reproduction" xlink:href="...">
    byAttribute(getModsRoot(data) \ "accessCondition", "type", "use and reproduction")
      .flatMap(node => node.attribute(node.getNamespace("xlink"), "href"))
      .flatMap(n => extractString(n.head))
      .map(_.trim)
      .map(URI)

  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] = {
    // <mods:location><mods:url usage="primary" access="object in context">
    val urls = getModsRoot(data) \ "location" \ "url"
    byAttribute(byAttribute(urls, "usage", "primary"), "access", "object in context")
      .flatMap(extractStrings)
      .map(stringOnlyWebResource)
  }

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] =
    Utils.formatXml(data)

  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    // TODO: The Dartmouth sample MODS records carry no explicit thumbnail/preview
    //  URL. Confirm the thumbnail source with the partner (candidates: the ark
    //  "Electronic image" identifier, or a derivable IIIF/thumbnail endpoint under
    //  collections.dartmouth.edu). For now, look for an access="preview" url.
    byAttribute(getModsRoot(data) \ "location" \ "url", "access", "preview")
      .flatMap(extractStrings)
      .map(stringOnlyWebResource)

  override def iiifManifest(data: Document[NodeSeq]): ZeroToMany[URI] = {
    // Archive (image/map) objects have a IIIF Presentation 3.0 manifest at
    //   https://collections.dartmouth.edu/archive/iiif/{collection}/{item}-mods.json
    // where {collection} is the host relatedItem's DRB recordIdentifier and {item}
    // is the record's own DRB recordIdentifier. Text collections (occom, teitexts)
    // have no such manifest, so we skip typeOfResource = "text".
    //
    // TODO: this hardcodes the collections.dartmouth.edu IIIF URL template. Ideally
    //  Dartmouth would emit the manifest URL explicitly in the MODS (e.g.
    //  <mods:location><mods:url note="iiifManifest">), as several DPLA hubs do.
    val root = getModsRoot(data)
    val isText = extractStrings(root \ "typeOfResource").exists(_.equalsIgnoreCase("text"))

    val collection = byAttribute(
      byAttribute(root \ "relatedItem", "type", "host") \ "recordInfo" \ "recordIdentifier",
      "source", "DRB"
    ).flatMap(extractStrings).headOption

    val item = byAttribute(root \ "recordInfo" \ "recordIdentifier", "source", "DRB")
      .flatMap(extractStrings)
      .headOption

    (for {
      c <- collection if !isText
      i <- item
    } yield URI(
      s"https://collections.dartmouth.edu/archive/iiif/$c/$i-mods.json"
    )).toSeq
  }

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = agent

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(data))

  // Helper methods

  def agent: EdmAgent = EdmAgent(
    name = Some("Dartmouth Libraries"),
    uri = Some(URI("http://dp.la/api/contributor/dartmouth"))
  )

  // Anchors to the record's root MODS element. For OAI-wrapped records that is
  // <metadata><mods:mods>; for raw records it is the top-level node itself. Using
  // an explicit path (rather than a descendant scan) avoids matching any nested
  // <mods:mods> inside a relatedItem.
  private def getModsRoot(data: Document[NodeSeq]): NodeSeq = {
    val ns: NodeSeq = data
    val wrapped = ns \ "metadata" \ "mods"
    if (wrapped.nonEmpty) wrapped else ns.filter(_.label == "mods")
  }

  // Filters a NodeSeq to the elements whose `attr` equals `value`.
  private def byAttribute(nodes: NodeSeq, attr: String, value: String): NodeSeq =
    nodes.flatMap(n => getByAttribute(n.asInstanceOf[Elem], attr, value))

  private def roleTerms(node: Node): Seq[String] =
    extractStrings(node \ "role" \ "roleTerm").map(_.trim.toLowerCase)

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
    // @valueURI -> exactMatch (the entity URI, e.g. an LC name authority);
    // @authorityURI -> scheme (the authority base). Only http(s) values are kept.
    val uri = getAttributeValue(node, "valueURI").filter(isHttpUri).map(URI).toSeq
    val scheme = getAttributeValue(node, "authorityURI").filter(isHttpUri).map(URI)
    EdmAgent(name = nameConstructor(node), exactMatch = uri, scheme = scheme)
  }

  private def isHttpUri(value: String): Boolean =
    value.startsWith("http://") || value.startsWith("https://")

  // Concatenates only a node's direct text children (ignoring nested elements
  // such as copyrightMD blocks), with whitespace collapsed.
  private def directText(node: Node): String =
    node.child.collect { case t: Text => t.text }.mkString(" ").reduceWhitespace

  private def skosConceptUriHelper(node: Node): SkosConcept = {
    val uri = getAttributeValue(node, "valueURI").map(URI).toSeq
    val scheme = getAttributeValue(node, "authorityURI").map(URI)
    val label = extractString(node)
    SkosConcept(providedLabel = label, exactMatch = uri, scheme = scheme)
  }
}
