package dpla.ingestion3.mappers.providers.experimental

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.messages.{IngestMessage, MessageCollector}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import org.scalatest.flatspec.AnyFlatSpec

import scala.xml.{NodeSeq, XML}

/** TEST HUB — see docs/ingestion/README_TEST_HUBS.md
  *
  * Fixtures are raw <mods:mods> records delivered by Dartmouth:
  *   - dartmouth.xml         : occom-765122 (a manuscript letter; text)
  *   - dartmouth-images.xml  : BCM images (still image; carries a CC rights URI)
  */
class DartmouthMappingTest extends AnyFlatSpec {

  implicit val msgCollector: MessageCollector[IngestMessage] =
    new MessageCollector[IngestMessage]

  val shortName = "dartmouth"

  val xml: Document[NodeSeq] =
    Document(XML.loadString(new FlatFileIO().readFileAsString("/dartmouth.xml")))
  val xmlImages: Document[NodeSeq] =
    Document(XML.loadString(new FlatFileIO().readFileAsString("/dartmouth-images.xml")))

  val extractor = new DartmouthMapping

  it should "use the provider shortname in minting IDs" in
    assert(extractor.useProviderName)

  it should "extract the correct original identifier" in
    assert(extractor.originalId(xml) === Some("occom-765122"))

  it should "extract the correct title" in
    assert(
      extractor.title(xml) ===
        Seq("Daniel Bull, letter, to Eleazar Wheelock, 1765 January 22")
    )

  it should "map only names with usage=primary as creator" in
    assert(extractor.creator(xml) === Seq(nameOnlyAgent("Bull, Daniel")))

  it should "not map contributor (dropped; open question for Dartmouth)" in
    assert(extractor.contributor(xml).isEmpty)

  it should "exclude non-primary names (addressee, repository) from creator" in {
    val names = extractor.creator(xml).flatMap(_.name)
    assert(!names.contains("Wheelock, Eleazar"))
    assert(!names.contains("Digital by Dartmouth Library"))
  }

  it should "map only the abstract to description (not mods:note)" in
    assert(
      extractor.description(xml) ===
        Seq("Bull writes that the Indian girl, whom Wheelock had committed to his care, has arrived.")
    )

  it should "hardcode dataProvider to Dartmouth Libraries" in {
    assert(extractor.dataProvider(xml) === Seq(nameOnlyAgent("Dartmouth Libraries")))
    assert(extractor.dataProvider(xmlImages) === Seq(nameOnlyAgent("Dartmouth Libraries")))
  }

  it should "exclude shareable=no abstracts from description" in {
    val d = extractor.description(xmlImages)
    assert(d.contains(
      "Associated images of cassette containing live concert recording of the " +
        "Barbary Coast Jazz Ensemble with Slide Hampton, Clint Houston, " +
        "Mickey Tucker, and Alan Dawson."
    ))
    assert(!d.contains("Part 1 of 4"))
  }

  it should "not leak the copyright holder into rights" in {
    val r = extractor.rights(xmlImages)
    assert(!r.contains("Trustees of Dartmouth College"))
    assert(r.contains("Creative Commons Attribution-NonCommercial License"))
  }

  it should "map the copyright holder to rightsHolder" in
    assert(
      extractor.rightsHolder(xmlImages) === Seq(nameOnlyAgent("Trustees of Dartmouth College"))
    )

  it should "prefer the analog dateCreated over the digitization dateIssued" in
    assert(extractor.date(xml) === Seq(stringOnlyTimeSpan("1765-01-22")))

  it should "extract the correct publisher" in
    assert(
      extractor.publisher(xml) === Seq(nameOnlyAgent("Trustees of Dartmouth College"))
    )

  it should "extract the correct place" in
    assert(extractor.place(xml) === Seq(nameOnlyPlace("Hanover, NH")))

  it should "extract the correct language" in
    assert(extractor.language(xml) === Seq(nameOnlyConcept("English")))

  it should "extract the correct type" in
    assert(extractor.`type`(xml) === Seq("text"))

  it should "extract the correct collection" in
    assert(extractor.collection(xml) === Seq(nameOnlyCollection("Occom Circle")))

  it should "extract the correct isShownAt" in
    assert(
      extractor.isShownAt(xml) === Seq(
        stringOnlyWebResource(
          "https://collections.dartmouth.edu/occom/html/diplomatic/765122-diplomatic.html"
        )
      )
    )

  it should "extract rights free-text from accessCondition" in
    assert(extractor.rights(xml).exists(_.startsWith("Copyright 2015 Trustees of Dartmouth College")))

  it should "extract no edmRights URI when none is present" in
    assert(extractor.edmRights(xml).isEmpty)

  it should "extract the edmRights URI from accessCondition/@xlink:href" in
    assert(
      extractor.edmRights(xmlImages) ===
        Seq(URI("https://creativecommons.org/licenses/by-nc/4.0/"))
    )

  it should "capture creator exactMatch (@valueURI) and scheme (@authorityURI)" in {
    val agentXml: Document[NodeSeq] = Document(
      <mods>
        <name usage="primary"
              valueURI="http://id.loc.gov/authorities/names/n79021164"
              authorityURI="http://id.loc.gov/authorities/names">
          <namePart>Whitman, Walt</namePart>
        </name>
      </mods>
    )
    assert(
      extractor.creator(agentXml) === Seq(
        EdmAgent(
          name = Some("Whitman, Walt"),
          exactMatch = Seq(URI("http://id.loc.gov/authorities/names/n79021164")),
          scheme = Some(URI("http://id.loc.gov/authorities/names"))
        )
      )
    )
  }

  it should "capture place exactMatch for http valueURIs but ignore FAST (OCoLC) codes" in {
    val placeXml: Document[NodeSeq] = Document(
      <mods>
        <subject valueURI="http://vocab.getty.edu/tgn/7013445">
          <geographic>Boston</geographic>
        </subject>
        <subject authority="fast" valueURI="(OCoLC)fst01204155">
          <geographic>United States</geographic>
        </subject>
      </mods>
    )
    assert(
      extractor.place(placeXml) === Seq(
        DplaPlace(name = Some("Boston"), exactMatch = Seq(URI("http://vocab.getty.edu/tgn/7013445"))),
        DplaPlace(name = Some("United States"))
      )
    )
  }

  it should "construct the IIIF manifest URL from the DRB recordIdentifiers" in
    assert(
      extractor.iiifManifest(xmlImages) === Seq(
        URI("https://collections.dartmouth.edu/archive/iiif/black-creative-music/BCM-19820213-hampton-images-mods.json")
      )
    )

  it should "not construct an IIIF manifest for text records" in
    assert(extractor.iiifManifest(xml).isEmpty)

  it should "have no creator when no name has usage=primary (images record)" in
    assert(extractor.creator(xmlImages).isEmpty)
}
