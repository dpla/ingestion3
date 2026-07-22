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

  it should "extract the correct creator (author role)" in
    assert(extractor.creator(xml) === Seq(nameOnlyAgent("Bull, Daniel")))

  it should "extract the correct contributor (non-creator role)" in
    assert(extractor.contributor(xml) === Seq(nameOnlyAgent("Wheelock, Eleazar")))

  it should "exclude the repository name from creator and contributor" in {
    val names =
      (extractor.creator(xml) ++ extractor.contributor(xml)).flatMap(_.name)
    assert(!names.contains("Digital by Dartmouth Library"))
  }

  it should "map only the abstract to description (not mods:note)" in
    assert(
      extractor.description(xml) ===
        Seq("Bull writes that the Indian girl, whom Wheelock had committed to his care, has arrived.")
    )

  it should "map dataProvider to the subLocation institution name (before the address)" in
    assert(
      extractor.dataProvider(xml) === Seq(nameOnlyAgent("Rauner Special Collections Library"))
    )

  it should "fall back to the repository name for dataProvider when no subLocation is present" in
    assert(
      extractor.dataProvider(xmlImages) === Seq(nameOnlyAgent("Digital by Dartmouth Library"))
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

  it should "construct the IIIF manifest URL from the DRB recordIdentifiers" in
    assert(
      extractor.iiifManifest(xmlImages) === Seq(
        URI("https://collections.dartmouth.edu/archive/iiif/black-creative-music/BCM-19820213-hampton-images-mods.json")
      )
    )

  it should "not construct an IIIF manifest for text records" in
    assert(extractor.iiifManifest(xml).isEmpty)

  it should "map all un-roled names as creators on the images record" in {
    val creators = extractor.creator(xmlImages).flatMap(_.name)
    assert(creators.contains("Hampton, Slide"))
    assert(!creators.contains("Digital by Dartmouth Library"))
  }
}
