package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.mappers.utils.{Document, XmlExtractor, XmlMapping}
import dpla.ingestion3.messages.IngestMessageTemplates
import dpla.ingestion3.model.DplaMapData.{ExactlyOne, ZeroToMany, ZeroToOne}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.{FlatFileIO, Utils}
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.xml.NodeSeq

/** Digital Maine (Maine State Library / Maine State Archives), harvested from
  * the Digital Commons (bepress) OAI-PMH feed at
  * https://digitalmaine.com/do/oai/ with metadataPrefix=qdc and the sets
  * publication:statedocs and publication:regions.
  *
  * Digital Commons' "qdc" is flat Dublin Core with dotted element names
  * (dc:description.abstract, dc:date.created, dc:coverage.spatial.lat, ...)
  * inside an oai_dc:dc wrapper.
  */
class MaineMapping
    extends XmlMapping
    with XmlExtractor
    with IngestMessageTemplates {

  // DO NOT CHANGE. DPLA item id = md5(getProviderName + "--" + originalId); the
  // "maine" salt is the one these items have had since ingestion1.
  override def useProviderName: Boolean = true

  override def getProviderName: Option[String] = Some("maine")

  // Records DPLA has carried since the 2017 ingestion1 harvest were harvested
  // through Maine State Library's REPOX aggregator, which wrapped each Digital
  // Commons identifier as oai:the.european.library.<repox set>:<identifier>.
  // To keep those DPLA item ids stable, records listed in the legacy crosswalk
  // use the wrapped identifier as originalId. Records added since 2017 use the
  // Digital Commons identifier as-is.
  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] =
    extractString(data \ "header" \ "identifier")
      .map(_.trim)
      .map(MaineMapping.legacyOriginalId)

  override def dplaUri(data: Document[NodeSeq]): ZeroToOne[URI] =
    mintDplaItemUri(data)

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(
      data
    ))

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] =
    Utils.formatXml(data)

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] =
    EdmAgent(
      name = Some("Digital Maine"),
      uri = Some(URI("http://dp.la/api/contributor/maine"))
    )

  // Digital Maine records the contributing institution in dc:contributor.
  // Records without one fail mapping (dataProvider is required); no default is
  // supplied here.
  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    values(data, "contributor").map(nameOnlyAgent)

  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    values(data, "identifier").map(stringOnlyWebResource)

  // Digital Commons puts the thumbnail URL in plain dc:description; the item
  // description is in dc:description.abstract.
  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    values(data, "description")
      .filter(isUrl)
      .map(stringOnlyWebResource)

  // edmRights takes only rightsstatements.org / creativecommons.org URIs as
  // published (shared Mapper.normalizeEdmRights cleans up their form); every
  // other dc:rights value, including other URLs, is kept as free-text rights.
  override def edmRights(data: Document[NodeSeq]): ZeroToMany[URI] =
    values(data, "rights").filter(isRightsUri).map(URI)

  override def rights(data: Document[NodeSeq]): ZeroToMany[String] =
    values(data, "rights").filterNot(isRightsUri)

  // SourceResource

  // The collection is the record's own Digital Commons publication, i.e. the set
  // publication:<collection> where <collection> is the identifier's prefix
  // (oai:digitalmaine.com:<collection>-<n>). Its title is the set name, which
  // the harvester records in <about><setName> (harvest.includeSetNames).
  override def collection(
      data: Document[NodeSeq]
  ): ZeroToMany[DcmiTypeCollection] = {
    val ownSet = extractString(data \ "header" \ "identifier")
      .map(_.trim)
      .collect { case MaineMapping.CollectionFromId(c) => s"publication:$c" }
    (data \ "about" \ "setName")
      .filter(node => ownSet.contains(node \@ "spec"))
      .flatMap(extractStrings)
      .map(nameOnlyCollection)
  }
  override def title(data: Document[NodeSeq]): ZeroToMany[String] =
    values(data, "title")

  override def creator(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    splitValues(data, "creator").map(nameOnlyAgent)

  override def description(data: Document[NodeSeq]): ZeroToMany[String] =
    values(data, "description.abstract") ++
      values(data, "description").filterNot(isUrl)

  override def date(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
    values(data, "date.created").map(stringOnlyTimeSpan)

  override def temporal(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
    values(data, "coverage.temporal").map(stringOnlyTimeSpan)

  override def extent(data: Document[NodeSeq]): ZeroToMany[String] =
    values(data, "format.extent")

  override def language(data: Document[NodeSeq]): ZeroToMany[SkosConcept] =
    splitValues(data, "language").map(nameOnlyConcept)

  // Place names come as separate dc:coverage.spatial elements; latitude and
  // longitude come as their own elements and are kept as a coordinates-only
  // place.
  override def place(data: Document[NodeSeq]): ZeroToMany[DplaPlace] = {
    val names = splitValues(data, "coverage.spatial").map(nameOnlyPlace)
    val coordinates = for {
      lat <- values(data, "coverage.spatial.lat").headOption
      long <- values(data, "coverage.spatial.long").headOption
    } yield DplaPlace(coordinates = Some(s"$lat, $long"))
    names ++ coordinates.toSeq
  }

  override def publisher(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    values(data, "publisher").map(nameOnlyAgent)

  override def subject(data: Document[NodeSeq]): ZeroToMany[SkosConcept] =
    splitValues(data, "subject").map(nameOnlyConcept)

  override def `type`(data: Document[NodeSeq]): ZeroToMany[String] =
    values(data, "type")

  // Helpers

  private def metadataRoot(data: Document[NodeSeq]): NodeSeq =
    data \ "metadata" \ "dc"

  private def values(data: Document[NodeSeq], element: String): Seq[String] =
    extractStrings(metadataRoot(data) \ element)
      .map(_.trim)
      .filter(_.nonEmpty)

  private def splitValues(
      data: Document[NodeSeq],
      element: String
  ): Seq[String] =
    values(data, element).flatMap(_.splitAtDelimiter(";"))

  // Only http(s) values are thumbnail links in dc:description.
  private def isUrl(value: String): Boolean =
    value.startsWith("http://") || value.startsWith("https://")

  private val RightsUri =
    "^https?://(www\\.)?(rightsstatements|creativecommons)\\.org/.*".r

  private def isRightsUri(value: String): Boolean =
    RightsUri.matches(value)
}

object MaineMapping {

  private val ResourcePath = "/maine/legacy_ids.tsv"

  private[providers] val CollectionFromId =
    "^oai:digitalmaine\\.com:(.+)-\\d+$".r

  /** Digital Commons OAI identifier -> REPOX set (msl_statedocs or msl_regions)
    * the record was harvested through in 2017. Built from the 2022 Maine JSONL
    * (s3://dpla-master-dataset/maine/jsonl/20220809_092608-maine-MAP3_1.IndexRecord.jsonl);
    * the third column is the DPLA id each entry must reproduce. Where an
    * identifier appeared in both REPOX sets, msl_statedocs is kept.
    *
    * Fails loudly if the resource is missing: an empty crosswalk would silently
    * renumber every legacy Maine item.
    */
  lazy val legacyRepoxSet: Map[String, String] = {
    // readFileAsSeq throws if the resource is missing.
    val entries = new FlatFileIO()
      .readFileAsSeq(ResourcePath)
      .drop(1) // header row
      .map(_.split("\t", 3))
      .map {
        case Array(id, set, _) if id.nonEmpty && set.nonEmpty => id -> set
        case other =>
          throw new IllegalStateException(
            s"Malformed line in $ResourcePath: ${other.mkString("\t")}"
          )
      }
      .toMap
    if (entries.isEmpty)
      throw new IllegalStateException(s"$ResourcePath is empty")
    entries
  }

  def legacyOriginalId(identifier: String): String =
    legacyRepoxSet.get(identifier) match {
      case Some(set) => s"oai:the.european.library.$set:$identifier"
      case None      => identifier
    }
}
