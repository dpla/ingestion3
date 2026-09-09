package dpla.ingestion3.mappers.providers

import dpla.ingestion3.executors.DplaMap
import dpla.ingestion3.mappers.utils.{Document, XmlExtractor, XmlMapping}
import dpla.ingestion3.messages.IngestMessageTemplates
import dpla.ingestion3.model.DplaMapData.{AtLeastOne, ExactlyOne, ZeroToMany, ZeroToOne}
import dpla.ingestion3.model.{DplaPlace, EdmAgent, EdmTimeSpan, EdmWebResource, LiteralOrUri, SkosConcept, URI, nameOnlyAgent, nameOnlyConcept, nameOnlyPlace, stringOnlyTimeSpan, stringOnlyWebResource}
import dpla.ingestion3.utils.{CHProviderRegistry, Utils}
import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.enrichments.normalizations.filters.ExtentIdentificationList
import org.json4s.JValue

import scala.xml._
import org.json4s.JsonDSL._

class IthakaMapping
    extends XmlMapping
    with XmlExtractor
    with IngestMessageTemplates {

  override def useProviderName: Boolean = true

  // DO NOT CHANGE. This is the id salt: DPLA item id = md5(getProviderName + "--" +
  // originalId). It must stay "artstor" (the original OCLC-era provider name) so the
  // JSTOR rebrand preserves the ids these items already have — regardless of whether
  // the ingest is invoked as jstor/artstor/ithaka. Changing it renumbers the entire hub.
  override def getProviderName: Option[String] = Some("artstor")

  // DPLA item id = md5(getProviderName + "--" + originalId). To keep the ids these
  // items had when they were harvested from the old Artstor (OCLC) feed, look the
  // JSTOR community id (the OAI header identifier) up in the pass-1 crosswalk and,
  // when found, use the legacy oai:oaicat.oclc.org id as originalId. Records with no
  // crosswalk entry (new since 2022) fall back to the community id -> a native id.
  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] =
    extractString(data \ "header" \ "identifier")
      .map(_.trim)
      .map(id => IthakaMapping.communityToOai.getOrElse(id, id))

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] =
    EdmAgent(
      // Display name is the rebrand (JSTOR). The contributor URI also moves to
      // jstor; this does NOT affect DPLA item ids (those hash getProviderName +
      // originalId, and getProviderName stays "artstor" for id continuity).
      uri = Some(URI("http://dp.la/api/contributor/jstor")),
      name = Some("JSTOR")
    )

  // The contributing institution is supplied by JSTOR in the OAI <about> block as
  // <oai_dc:publisher> (the harvester preserves <about>). Mapped as given, with no
  // normalization. This is distinct from the work's actual publisher, which JSTOR
  // puts in <metadata><oai_dc:publisher> and which maps to sourceResource.publisher.
  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    extractStrings(data \ "about" \ "dc" \ "publisher")
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(nameOnlyAgent)

  override def dplaUri(data: Document[NodeSeq]): ZeroToOne[URI] =
    mintDplaItemUri(data)

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] =
    Utils.formatXml(data)

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(
      data
    ))

  // JSTOR crams MARC-relator-style role terms into flat dc:contributor, each role
  // element preceding the name(s) it qualifies, e.g.
  //   ["photographer", "Conzo, Joe", "publisher", "Antonio Zatta"].
  // Walk the elements in order and route each NAME to the field its preceding role
  // implies: creator-type roles -> creator, "publisher" -> publisher, and everything
  // else (unroled names, other role words, and any dangling role word with no
  // following name) -> contributor. Nothing is discarded.
  private val creatorRoleTerms: Set[String] = Set(
    "creator", "creators", "photographer", "photographers", "illustrator",
    "illustrators", "artist", "artists", "author", "authors", "painter",
    "printmaker", "engraver", "designer", "graphic designer", "architect"
  )

  private def partitionContributors(
      data: Document[NodeSeq]
  ): (Seq[String], Seq[String], Seq[String]) = {
    val creators = scala.collection.mutable.ArrayBuffer.empty[String]
    val publishers = scala.collection.mutable.ArrayBuffer.empty[String]
    val contributors = scala.collection.mutable.ArrayBuffer.empty[String]
    // Routing target set by a role term; the role word is retained so a dangling
    // role (no following name) still falls through to contributor rather than drop.
    var pending: Option[(scala.collection.mutable.ArrayBuffer[String], String)] = None
    def flushDangling(): Unit =
      pending.foreach { case (_, word) => contributors += word }

    extractStrings(data \ "metadata" \ "dc" \ "contributor")
      .map(_.trim)
      .filter(_.nonEmpty)
      .foreach { element =>
        val lc = element.toLowerCase
        if (creatorRoleTerms.contains(lc)) {
          flushDangling(); pending = Some((creators, element))
        } else if (lc == "publisher") {
          flushDangling(); pending = Some((publishers, element))
        } else {
          val target = pending.map(_._1).getOrElse(contributors)
          element
            .splitAtDelimiter("\\|")
            .flatMap(_.splitAtDelimiter(";"))
            .map(_.trim)
            .filter(_.nonEmpty)
            .foreach(target += _)
          pending = None
        }
      }
    flushDangling()
    (creators.toSeq, publishers.toSeq, contributors.toSeq)
  }

  // Drop empty and placeholder "unknown" agent names.
  private def keepName(x: String): Boolean =
    x.nonEmpty && !x.equalsIgnoreCase("unknown")

  override def contributor(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    partitionContributors(data)._3
      .filter(keepName)
      .map(nameOnlyAgent)

  // Work publisher: dc:publisher (metadata) plus any publisher-role names carried
  // in dc:contributor (routed by partitionContributors).
  override def publisher(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    (extractStrings(data \ "metadata" \ "dc" \ "publisher") ++
      partitionContributors(data)._2)
      .filter(keepName)
      .map(nameOnlyAgent)

  // JSTOR omits dc:title for genuinely untitled items but displays "[No title]" in
  // its own UI. Title is a required field, so mirror JSTOR's convention with a
  // "[No title]" fallback rather than failing these records.
  override def title(data: Document[NodeSeq]): AtLeastOne[String] = {
    val titles =
      extractStrings(data \ "metadata" \ "dc" \ "title").map(_.trim).filter(_.nonEmpty)
    if (titles.nonEmpty) titles else Seq("[No title]")
  }

  // Creator: dc:creator (metadata) plus any creator-role names carried in
  // dc:contributor (routed by partitionContributors).
  override def creator(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    (extractStrings(data \ "metadata" \ "dc" \ "creator")
      .flatMap(_.splitAtDelimiter("\\|"))
      .flatMap(_.splitAtDelimiter(";")) ++
      partitionContributors(data)._1)
      .map(_.trim)
      .filter(keepName)
      .map(nameOnlyAgent)

  override def subject(data: Document[NodeSeq]): ZeroToMany[SkosConcept] =
    extractStrings(data \ "metadata" \ "dc" \ "subject")
      .flatMap(_.splitAtDelimiter("\\|"))
      .flatMap(_.splitAtDelimiter(";"))
      .filter(_.nonEmpty)
      .map(nameOnlyConcept)

  override def description(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(data \ "metadata" \ "dc" \ "description")
      .map(_.trim)
      .filter(_.nonEmpty)

  override def date(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
    extractStrings(data \ "metadata" \ "dc" \ "date")
      .flatMap(_.splitAtDelimiter("\\|"))
      .flatMap(_.splitAtDelimiter(";"))
      .filter(_.nonEmpty)
      .map(stringOnlyTimeSpan)

  private def typeyFormaty(data: Document[NodeSeq]): ZeroToMany[String] =
    (extractStrings(data \ "metadata" \ "dc" \ "type") ++
      extractStrings(data \ "metadata" \ "dc" \ "format"))
      .flatMap(_.splitAtDelimiter("\\|"))
      .flatMap(_.splitAtDelimiter(";"))
      .filter(_.nonEmpty)

  // todo make sure these values look ok
  override def `type`(data: Document[NodeSeq]): ZeroToMany[String] =
    typeyFormaty(data)

  // The block/allow filters blank out (return "") the terms they exclude, so
  // drop those empties rather than emit stray "" values in format/extent.
  override def format(data: Document[NodeSeq]): ZeroToMany[String] =
    typeyFormaty(data)
      .map(_.applyBlockFilter(ExtentIdentificationList.termList))
      .filter(_.nonEmpty)

  override def extent(data: Document[NodeSeq]): ZeroToMany[String] =
    typeyFormaty(data)
      .map(_.applyAllowFilter(ExtentIdentificationList.termList))
      .filter(_.nonEmpty)

  override def identifier(data: Document[NodeSeq]): ZeroToMany[String] =
    // JSTOR's persistent record id is the OAI header identifier (the community
    // id); surface it first, then the record's own non-media dc:identifier values
    // (local call numbers / accession ids). The media/viewer-prefixed identifiers
    // are excluded — they are handled by preview, mediaMaster, and isShownAt.
    extractString(data \ "header" \ "identifier")
      .map(_.trim)
      .filter(_.nonEmpty)
      .toSeq ++
      extractStrings(data \ "metadata" \ "dc" \ "identifier")
        .flatMap(_.splitAtDelimiter(";"))
        .filter(x =>
          x.nonEmpty &&
            !x.startsWith("Thumbnail:") &&
            !x.startsWith("Medias:") &&
            !x.startsWith("Media:") &&
            !x.startsWith("FullSize:") &&
            !x.startsWith("ADLImageViewer:") &&
            !x.startsWith("SSCImageViewer:")
        )

  // isShownAt is built from the community id (the OAI header identifier), which is
  // the canonical JSTOR item page:
  //   https://www.jstor.org/stable/10.2307/community.<id>
  // That is exactly what the feed's ADLImageViewer/SSCImageViewer identifiers
  // contain when present (both are identical to each other and to this URL), but
  // constructing it from the header id yields one value for EVERY record —
  // including those with no viewer identifier, which would otherwise be dropped
  // for a missing required isShownAt. (To verify against the full harvest: the
  // header id has 100% coverage, and equals the ADL/SSC community id wherever
  // those are present.)
  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    extractString(data \ "header" \ "identifier")
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(id =>
        stringOnlyWebResource(
          s"https://www.jstor.org/stable/10.2307/community.$id"
        )
      )
      .toSeq

  // Thumbnail. JSTOR emits it as a dc:identifier prefixed "Thumbnail:", e.g.
  // "Thumbnail:https://forum.jstor.org/oai/thumbnail?id=2841861". Maps to preview
  // (the DPLA thumbnail); the "Thumbnail:" prefix is stripped and it is filtered
  // out of `identifier`.
  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    extractStrings(data \ "metadata" \ "dc" \ "identifier")
      .map(_.trim)
      .filter(_.startsWith("Thumbnail:"))
      .map(_.stripPrefix("Thumbnail:").trim)
      .filter(_.nonEmpty)
      .map(stringOnlyWebResource)

  // Master (full-size) media. JSTOR's OAI record only carries a "Medias:" listing
  // *endpoint*, not the media URLs themselves (and its "FullSize:" URL is
  // paywalled). The two-step harvest resolves each record's Medias endpoint and
  // injects the resulting public media URLs back into the harvested document as
  // dc:identifier values prefixed "Media:" (ordered by the listing's
  // sequence_number). Here we read those, so mapping stays offline. mediaMaster is
  // ZeroToMany, so multi-image objects map all their media in order.
  override def mediaMaster(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    extractStrings(data \ "metadata" \ "dc" \ "identifier")
      .map(_.trim)
      .filter(_.startsWith("Media:"))
      .map(_.stripPrefix("Media:").trim)
      .filter(_.nonEmpty)
      .map(stringOnlyWebResource)

  override def relation(data: Document[NodeSeq]): ZeroToMany[LiteralOrUri] =
    extractStrings(data \ "metadata" \ "dc" \ "relation")
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(LiteralOrUri(_, isUri = false))

  override def language(data: Document[NodeSeq]): ZeroToMany[SkosConcept] =
    extractStrings(data \ "metadata" \ "dc" \ "language")
      .flatMap(_.splitAtDelimiter("\\|"))
      .flatMap(_.splitAtDelimiter(";"))
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(nameOnlyConcept)

  override def place(data: Document[NodeSeq]): ZeroToMany[DplaPlace] =
    extractStrings(data \ "metadata" \ "dc" \ "coverage")
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(nameOnlyPlace)

  override def rights(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(data \ "metadata" \ "dc" \ "rights")
      .map(_.trim)
      .filter(_.nonEmpty)

}

object IthakaMapping extends IthakaMapping {

  /** JSTOR community id (OAI header identifier) -> legacy oai:oaicat.oclc.org id,
    * from the pass-1 (media-derived) crosswalk built off the 2022 Artstor index.
    * Used by originalId so re-harvested JSTOR Forum records keep the DPLA item ids
    * they had under the old feed. Loaded once per JVM from the packaged resource
    * src/main/resources/jstor/community_to_oai.tsv (pass-2 redirect-derived entries
    * are intentionally excluded — those community numbers are not OAI-addressable). */
  lazy val communityToOai: Map[String, String] = {
    val stream = getClass.getResourceAsStream("/jstor/community_to_oai.tsv")
    if (stream == null) Map.empty
    else {
      val src = scala.io.Source.fromInputStream(stream, "UTF-8")
      try
        src
          .getLines()
          .drop(1) // header row
          .flatMap { line =>
            line.split("\t", 2) match {
              case Array(c, o) if c.trim.nonEmpty && o.trim.nonEmpty =>
                Some(c.trim -> o.trim)
              case _ => None
            }
          }
          .toMap
      finally src.close()
    }
  }

  def main(args: Array[String]): Unit = {

    val dplaMap = new DplaMap()
    val extractorClass = CHProviderRegistry.lookupProfile("ithaka").get
    val oreAggregation = dplaMap.map(record.toString(), extractorClass)
    oreAggregation.sourceResource.title.foreach(println)
  }

  val record: Elem =
    <record>
      <header xmlns="http://www.openarchives.org/OAI/2.0/"
              xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
        <identifier>2841861</identifier>
        <datestamp>2025-02-13T14:49:13Z</datestamp>
      </header>
      <metadata xmlns="http://www.openarchives.org/OAI/2.0/"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
        <oai_dc:dc
        xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd"
        xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"
        xmlns:dc="http://www.openarchives.org/OAI/2.0/oai_dc/">
          <oai_dc:title> Hooigracht. 's GRAVENHAGE. [Hooigracht (canal), The Hague]; verso: Grand
            Bazar de la Paix. [divided back, no message] </oai_dc:title>
          <oai_dc:title>overall</oai_dc:title>
          <oai_dc:title>recto</oai_dc:title>
          <oai_dc:creator> Probably Grand Bazar de la Paix (publisher, Dutch, act. ca.1907-1914) </oai_dc:creator>
          <oai_dc:subject> Postcards; Netherlands; Holland (historical region); Architecture;
            Hague (The) (Zuid-Holland, Netherlands); Canals </oai_dc:subject>
          <oai_dc:date>ca.1907-1914 (publication date)</oai_dc:date>
          <oai_dc:type>Picture postcard</oai_dc:type>
          <oai_dc:format>cardstock|paper</oai_dc:format>
          <oai_dc:format>9 x 14 cm (3.54 x 5.51 inches) approximately</oai_dc:format>
          <oai_dc:identifier>Box 12.54-10</oai_dc:identifier>
          <oai_dc:identifier>534724</oai_dc:identifier>
          <oai_dc:identifier> Thumbnail:https://forum.jstor.org/oai/thumbnail?id=2841861 </oai_dc:identifier>
          <oai_dc:identifier> Medias:https://forum.jstor.org/oai/medias/2841861 </oai_dc:identifier>
          <oai_dc:identifier>
            FullSize:https://forum.jstor.org/assets/2841861/representation/size/9 </oai_dc:identifier>
          <oai_dc:identifier>
            ADLImageViewer:https://www.jstor.org/stable/10.2307/community.2841861 </oai_dc:identifier>
          <oai_dc:identifier>
            SSCImageViewer:https://www.jstor.org/stable/10.2307/community.2841861 </oai_dc:identifier>
          <oai_dc:source>Trinity College Library</oai_dc:source>
          <oai_dc:coverage>Dutch</oai_dc:coverage>
          <oai_dc:coverage> Trinity College, Watkinson Library (Hartford, Connecticut, USA) </oai_dc:coverage>
          <oai_dc:rights> This digital collection and its contents are made available by Trinity
            College Library for limited noncommercial, educational, and personal use only. For
            other uses, or for additional information regarding the collection, contact the
            staff of Watkinson Library, Trinity College, Hartford, CT 06106. </oai_dc:rights>
        </oai_dc:dc>
      </metadata>
      <about>
        <request verb="ListRecords" metadataPrefix="oai_dc" set="580" />
        <resumptionToken> set%3D580%26metadataPrefix%3Doai_dc%26batch_size%3D11%26cursor%3D100 </resumptionToken>
        <responseDate>2025-04-09T18:58:37.104Z</responseDate>
      </about>
    </record>
}
