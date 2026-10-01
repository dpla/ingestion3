package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.messages.{IngestMessage, MessageCollector}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import org.scalatest.flatspec.AnyFlatSpec

import scala.xml.{NodeSeq, XML}

/** See docs/ingestion/dartmouth-mapping-draft.md
  *
  * Fixtures are real OAI-wrapped MODS records from the live feed
  * (https://collections.dartmouth.edu/archive/oai, metadataPrefix=mods):
  *   - dartmouth-maps.xml   : granite-state-maps NH_1638_001 (ark; relatedItem
  *                            @type="original"; cartographics/coordinates; FAST)
  *   - dartmouth-poster.xml : winter-carnival-posters dwcposters-1911-001 (doi;
  *                            relatedItem @type="otherFormat")
  *   - dartmouth-bcm.xml    : black-creative-music hampton-images (ark; no primary
  *                            name -> no creator; multiple contributors)
  */
class DartmouthMappingTest extends AnyFlatSpec {

  implicit val msgCollector: MessageCollector[IngestMessage] =
    new MessageCollector[IngestMessage]

  private def doc(resource: String): Document[NodeSeq] =
    Document(XML.loadString(new FlatFileIO().readFileAsString(resource)))

  private def inline(mods: scala.xml.Elem): Document[NodeSeq] = Document(mods)

  val maps: Document[NodeSeq] = doc("/dartmouth-maps.xml")
  val poster: Document[NodeSeq] = doc("/dartmouth-poster.xml")
  val bcm: Document[NodeSeq] = doc("/dartmouth-bcm.xml")

  val extractor = new DartmouthMapping

  // ── IDs & provider ────────────────────────────────────────────────────────

  it should "use the provider shortname in minting IDs" in
    assert(extractor.useProviderName)

  it should "extract the DRB original identifier" in
    assert(extractor.originalId(maps) === Some("NH_1638_001"))

  it should "hardcode provider and dataProvider to Dartmouth Libraries" in {
    assert(extractor.provider(maps).name === Some("Dartmouth Libraries"))
    assert(extractor.dataProvider(maps) === Seq(nameOnlyAgent("Dartmouth Libraries")))
  }

  // ── isShownAt: doi > ark > primary; drop other identifiers ──────────────────

  it should "map isShownAt to the ark when no doi is present" in
    assert(
      extractor.isShownAt(maps) ===
        Seq(stringOnlyWebResource("https://n2t.net/ark:/83024/d4g44hw6f"))
    )

  it should "map isShownAt to the doi when present" in
    assert(
      extractor.isShownAt(poster) ===
        Seq(stringOnlyWebResource("https://doi.org/10.1349/ddlp.1284"))
    )

  it should "prefer doi over ark when a record has both" in {
    val d = inline(
      <mods xmlns="http://www.loc.gov/mods/v3">
        <identifier type="ark">https://n2t.net/ark:/83024/zzz</identifier>
        <identifier type="doi">https://doi.org/10.1/abc</identifier>
      </mods>
    )
    assert(extractor.isShownAt(d) === Seq(stringOnlyWebResource("https://doi.org/10.1/abc")))
  }

  it should "normalize bare doi:/ark: isShownAt values to resolvable URLs" in {
    val d = inline(
      <mods xmlns="http://www.loc.gov/mods/v3">
        <identifier type="ark">ark:/83024/abc</identifier>
      </mods>
    )
    assert(extractor.isShownAt(d) === Seq(stringOnlyWebResource("https://n2t.net/ark:/83024/abc")))
  }

  it should "drop invalid and non-doi/ark identifiers from the mapping" in {
    // maps.xml carries identifier[@type="uri" invalid="yes"] and type="ark"
    assert(extractor.identifier(maps).isEmpty)
    assert(!extractor.isShownAt(maps).exists(_.uri.toString.contains("libarchive")))
  }

  // ── preview / iiifManifest ──────────────────────────────────────────────────

  it should "resolve a relative preview path against the base URL" in
    assert(
      extractor.preview(maps) === Seq(
        stringOnlyWebResource(
          "https://collections.dartmouth.edu/xcdas-derivative/granite-state-maps/jpeg-160x120/NH_1638_001.jpg"
        )
      )
    )

  it should "map iiifManifest from location/url[@note='IIIF manifest']" in
    assert(
      extractor.iiifManifest(maps) === Seq(
        URI("https://collections.dartmouth.edu/archive/iiif/granite-state-maps/NH_1638_001-mods.json")
      )
    )

  // ── Dates: relatedItem original/otherFormat over top-level ──────────────────

  it should "prefer the relatedItem[@type=original] date over the digitization date" in
    // top-level dateIssued is 2015 (digitization); original is 1638
    assert(extractor.date(maps) === Seq(stringOnlyTimeSpan("1638")))

  it should "prefer the relatedItem[@type=otherFormat] date (poster: 1911 not 2014)" in
    assert(extractor.date(poster) === Seq(stringOnlyTimeSpan("1911")))

  it should "prefer otherFormat dateCreated (bcm: 1982-02-13 not 2025)" in
    assert(extractor.date(bcm) === Seq(stringOnlyTimeSpan("1982-02-13")))

  // ── Names: creator=primary; contributor=non-primary minus repository ────────

  it should "map usage=primary names to creator" in
    assert(extractor.creator(maps) === Seq(nameOnlyAgent("Gardner, John, 1624-1706")))

  it should "map non-primary names to contributor, excluding the repository role" in {
    val names = extractor.contributor(maps).flatMap(_.name)
    assert(names.contains("Putnam, Charles A."))
    assert(!names.contains("Dartmouth Digital Library Program")) // repository excluded
  }

  it should "have no creator and multiple contributors when no name is primary (bcm)" in {
    assert(extractor.creator(bcm).isEmpty)
    val names = extractor.contributor(bcm).flatMap(_.name)
    assert(names.contains("Hampton, Slide"))
    assert(names.contains("Barbary Coast Jazz Ensemble"))
    assert(!names.contains("Digital by Dartmouth Library")) // repository excluded
    assert(names.size === 5)
  }

  it should "exclude a repository identified by the MARC relator code rps" in {
    val d = inline(
      <mods xmlns="http://www.loc.gov/mods/v3">
        <name><namePart>Repo Co</namePart>
          <role><roleTerm type="code" authority="marcrelator">rps</roleTerm></role>
        </name>
        <name><namePart>Real Contributor</namePart></name>
      </mods>
    )
    assert(extractor.contributor(d).flatMap(_.name) === Seq("Real Contributor"))
  }

  // ── genre -> genre with FAST normalization ──────────────────────────────────

  it should "map genre to genre, converting bare FAST codes to FAST URIs" in {
    val uris = extractor.genre(maps).flatMap(_.exactMatch).map(_.toString)
    assert(uris.contains("http://id.worldcat.org/fast/1752699")) // Digital maps
    assert(uris.contains("http://id.worldcat.org/fast/1423704")) // Maps
    assert(extractor.genre(maps).flatMap(_.providedLabel).exists(_.equalsIgnoreCase("map")))
  }

  // ── place: coordinates + geographic FAST exactMatch ─────────────────────────

  it should "map cartographics/coordinates as-is (MARC-255 string)" in {
    val d = inline(
      <mods xmlns="http://www.loc.gov/mods/v3">
        <subject><cartographics><coordinates>(W 73°--W 70°/N 45°15ʹ--N 42°30ʹ).</coordinates></cartographics></subject>
      </mods>
    )
    assert(extractor.place(d).exists(_.coordinates.exists(_.startsWith("(W 73"))))
  }

  it should "map hierarchicalGeographic to a structured place" in {
    val d = inline(
      <mods xmlns="http://www.loc.gov/mods/v3">
        <subject><hierarchicalGeographic>
          <country>United States</country><state>New Hampshire</state>
          <county>Grafton</county><city>Hanover</city>
        </hierarchicalGeographic></subject>
      </mods>
    )
    assert(extractor.place(d).exists(x => x.state.contains("New Hampshire") && x.city.contains("Hanover")))
  }

  it should "convert a bare FAST subject valueURI to a FAST URI exactMatch on place" in {
    val geo = extractor.place(maps).filter(_.name.contains("United States"))
    assert(geo.nonEmpty)
    assert(geo.exists(_.exactMatch.map(_.toString).contains("http://id.worldcat.org/fast/1310063")))
  }

  // ── rights / edmRights / rightsHolder ───────────────────────────────────────

  it should "map the standardized rights URI to edmRights (maps: rightsstatements)" in
    assert(
      extractor.edmRights(maps) === Seq(URI("http://rightsstatements.org/vocab/NoC-US/1.0/"))
    )

  it should "map the standardized rights URI to edmRights (poster)" in
    assert(extractor.edmRights(poster) === Seq(URI("http://rightsstatements.org/vocab/NoC-US/1.0/")))

  it should "map a Creative Commons standardized rights URI, canonicalizing https to http" in {
    // DPLA's edmRights vocabulary is http-only; an https CC URI must be
    // normalized to http or it fails edmRights validation and is dropped.
    val d = inline(
      <mods xmlns="http://www.loc.gov/mods/v3" xmlns:xlink="http://www.w3.org/1999/xlink">
        <accessCondition type="use and reproduction" displayLabel="Standardized rights statement" xlink:href="https://creativecommons.org/licenses/by-nc/4.0/">CC BY-NC</accessCondition>
      </mods>
    )
    assert(extractor.edmRights(d) === Seq(URI("http://creativecommons.org/licenses/by-nc/4.0/")))
  }

  it should "canonicalize an https rightsstatements.org URI to http" in {
    val d = inline(
      <mods xmlns="http://www.loc.gov/mods/v3" xmlns:xlink="http://www.w3.org/1999/xlink">
        <accessCondition type="use and reproduction" xlink:href="https://rightsstatements.org/vocab/InC/1.0/">In Copyright</accessCondition>
      </mods>
    )
    assert(extractor.edmRights(d) === Seq(URI("http://rightsstatements.org/vocab/InC/1.0/")))
  }

  it should "keep accessCondition free text as rights" in
    assert(extractor.rights(maps).exists(_.toLowerCase.contains("public domain")))

  it should "map a copyrightMD rights holder to rightsHolder" in {
    val d = inline(
      <mods xmlns="http://www.loc.gov/mods/v3" xmlns:cmd="http://www.cdlib.org/inside/diglib/copyrightMD">
        <accessCondition>
          <cmd:copyright><cmd:rights.holder><cmd:name>Trustees of Dartmouth College</cmd:name></cmd:rights.holder></cmd:copyright>
        </accessCondition>
      </mods>
    )
    assert(extractor.rightsHolder(d) === Seq(nameOnlyAgent("Trustees of Dartmouth College")))
  }
}
