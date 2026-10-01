package dpla.ingestion3.mappers.providers.experimental

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.messages.{IngestMessage, MessageCollector}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import org.scalatest.flatspec.AnyFlatSpec

import scala.xml.{NodeSeq, XML}

/** TEST HUB — see docs/ingestion/hbcula-mapping-draft.md
  *
  * Fixtures are real OAI-PMH oai_qdc records from the live CONTENTdm feed
  * (https://hbcudigitallibrary.auctr.edu/oai/oai.php), harvested 2026-10-01:
  *   - hbcula-vsud.xml : VSUD/115 — complete record (dc:source, creator, spatial)
  *   - hbcula-rwwl.xml : rwwl/4   — set omits dc:source (no dataProvider);
  *                       local file identifier; dcterms:isPartOf
  *   - hbcula-becu.xml : becu/3   — dc:relation; set omits dc:source
  *   - hbcula-psua.xml : psua/54  — multi-part dc:source with URLs; trailing tab in title
  *   - hbcula-suam.xml : suam/989 — no dc:rights
  */
class HbculaMappingTest extends AnyFlatSpec {

  implicit val msgCollector: MessageCollector[IngestMessage] =
    new MessageCollector[IngestMessage]

  private def doc(resource: String): Document[NodeSeq] =
    Document(XML.loadString(new FlatFileIO().readFileAsString(resource)))

  val vsud: Document[NodeSeq] = doc("/hbcula-vsud.xml")
  val rwwl: Document[NodeSeq] = doc("/hbcula-rwwl.xml")
  val becu: Document[NodeSeq] = doc("/hbcula-becu.xml")
  val psua: Document[NodeSeq] = doc("/hbcula-psua.xml")
  val suam: Document[NodeSeq] = doc("/hbcula-suam.xml")

  val extractor = new HbculaMapping

  // ── IDs & provider ────────────────────────────────────────────────────────

  it should "use the provider shortname in minting IDs" in
    assert(extractor.useProviderName)

  it should "extract the OAI header identifier as originalId" in
    assert(
      extractor.originalId(vsud) ===
        Some("oai:hbcudigitallibrary.auctr.edu:VSUD/115")
    )

  it should "hardcode the provider to HBCU Library Alliance" in
    assert(extractor.provider(vsud).name === Some("HBCU Library Alliance"))

  // ── dataProvider: dc:source as given, first value, no fallback ─────────────

  it should "map dataProvider from dc:source" in
    assert(
      extractor.dataProvider(vsud) ===
        Seq(nameOnlyAgent("Virginia State University Special Collections and Archives"))
    )

  it should "leave dataProvider empty when the set omits dc:source (no isPartOf fallback)" in {
    assert(extractor.dataProvider(rwwl) === Seq())
    assert(extractor.dataProvider(becu) === Seq())
  }

  it should "take the first ;-delimited dc:source value as given (no cleanup)" in
    assert(
      extractor.dataProvider(psua) ===
        Seq(nameOnlyAgent("Library: https://libguides.philander.edu/home"))
    )

  // ── Web resources ─────────────────────────────────────────────────────────

  it should "map isShownAt to the CONTENTdm URL exactly as given" in
    assert(
      extractor.isShownAt(rwwl) ===
        Seq(stringOnlyWebResource("http://hbcudigitallibrary.auctr.edu/cdm/ref/collection/rwwl/id/4"))
    )

  it should "construct an https CONTENTdm thumbnail for preview" in
    assert(
      extractor.preview(vsud) ===
        Seq(stringOnlyWebResource(
          "https://hbcudigitallibrary.auctr.edu/utils/getthumbnail/collection/VSUD/id/115"
        ))
    )

  it should "emit no preview when there is no http identifier" in {
    val d: Document[NodeSeq] = Document(
      <record xmlns="http://www.openarchives.org/OAI/2.0/">
        <metadata><dc:identifier xmlns:dc="http://purl.org/dc/elements/1.1/">becu.0117</dc:identifier></metadata>
      </record>
    )
    assert(extractor.preview(d) === Seq())
    assert(extractor.isShownAt(d) === Seq())
  }

  // ── SourceResource ────────────────────────────────────────────────────────

  it should "map title, stripping a trailing period" in {
    assert(extractor.title(vsud) === Seq("Audience Hall, 1888"))
    val d: Document[NodeSeq] = Document(
      <record xmlns="http://www.openarchives.org/OAI/2.0/">
        <metadata><dc:title xmlns:dc="http://purl.org/dc/elements/1.1/">North Hall.	</dc:title></metadata>
      </record>
    )
    assert(extractor.title(d) === Seq("North Hall"))
  }

  it should "map all dc:identifier values as given" in
    assert(
      extractor.identifier(rwwl) === Seq(
        "auc.001.bgx3.00000000.pho0039.jpg",
        "http://hbcudigitallibrary.auctr.edu/cdm/ref/collection/rwwl/id/4"
      )
    )

  it should "map dc:relation" in
    assert(
      extractor.relation(becu) ===
        Seq(eitherStringOrUri("https://www.cookman.edu/library/index.html"))
    )

  it should "split subjects on ; and drop empty segments" in
    assert(
      extractor.subject(rwwl).flatMap(_.providedLabel) === Seq(
        "Buildings",
        "Dormitories",
        "Universities & colleges",
        "Classrooms",
        "Educational facilities"
      )
    )

  it should "map creator, date, place, language and type" in {
    assert(extractor.creator(vsud) === Seq(nameOnlyAgent("Egan, Christopher")))
    assert(extractor.date(becu) === Seq(stringOnlyTimeSpan("1983-1984")))
    assert(extractor.place(vsud) === Seq(nameOnlyPlace("Virginia--Petersburg")))
    assert(extractor.language(vsud) === Seq(nameOnlyConcept("eng")))
    assert(extractor.`type`(vsud) === Seq("Black & white photographs"))
  }

  it should "map format, stripping trailing punctuation and whitespace" in
    assert(extractor.format(psua) === Seq("application/pdf"))

  it should "map dcterms:isPartOf to collection" in
    assert(extractor.collection(rwwl) === Seq(nameOnlyCollection("Atlanta University Photographs")))

  // ── Rights: free text only; no URI in the feed ────────────────────────────

  it should "map dc:rights as free text" in
    assert(extractor.rights(vsud).head.startsWith("Copyright VSU Library and Media Services."))

  it should "leave rights empty when the record has no dc:rights" in
    assert(extractor.rights(suam) === Seq())

  it should "not map edmRights (the feed carries no rights URI)" in
    assert(extractor.edmRights(vsud) === Seq())
}
