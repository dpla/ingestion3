package dpla.ingestion3.mappers.providers.experimental

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.messages.{IngestMessage, MessageCollector}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import org.scalatest.flatspec.AnyFlatSpec

import scala.xml.{NodeSeq, XML}

/** TEST HUB — see docs/ingestion/README_TEST_HUBS.md
  *
  * Fixtures are PBCore <pbcoreDescriptionDocument> records:
  *   - aapb.xml            : real ORR record (UH-Baylor Football); has rights, ARK,
  *                           ";"-delimited subjects, Online Reading Room access.
  *   - aapb-notonline.xml  : real record (Market to Market); has rights but is NOT
  *                           Online Reading Room -> no thumbnail.
  *   - aapb-footrace.xml   : pbcore.org sample (The Great American Footrace); a
  *                           single titleType="Title" plus multiple creators.
  *   - aapb-episode.xml    : pbcore.org sample (Prospects of Mankind); Series +
  *                           Episode titles, AAPB Format/Topical genres, contributors.
  */
class AapbMappingTest extends AnyFlatSpec {

  implicit val msgCollector: MessageCollector[IngestMessage] =
    new MessageCollector[IngestMessage]

  val extractor = new AapbMapping

  private def doc(file: String): Document[NodeSeq] =
    Document(XML.loadString(new FlatFileIO().readFileAsString(file)))

  val rawFootage: Document[NodeSeq] = doc("/aapb.xml")
  val notOnline: Document[NodeSeq] = doc("/aapb-notonline.xml")
  val footrace: Document[NodeSeq] = doc("/aapb-footrace.xml")
  val episode: Document[NodeSeq] = doc("/aapb-episode.xml")

  it should "use the provider shortname in minting IDs" in
    assert(extractor.useProviderName)

  it should "extract the AACIP identifier as originalId" in
    assert(extractor.originalId(rawFootage) === Some("cpb-aacip/513-000000145w"))

  // Titles

  it should "use an explicit primary title when present" in
    assert(extractor.title(footrace) === Seq("The Great American Footrace"))

  it should "combine Series + Episode into a title when there is no primary title" in
    assert(
      extractor.title(episode) === Seq(
        "Prospects of Mankind with Eleanor Roosevelt; New Possibilities for Coexistence"
      )
    )

  it should "fall back to the Series title alone when there is no episode" in
    assert(extractor.title(notOnline) === Seq("Market to Market 3503H"))

  it should "fall back to any title when the titleType is unusual" in
    assert(extractor.title(rawFootage) === Seq("UH-Baylor Football"))

  // isShownAt — landing page from the underscore form of the id

  it should "construct isShownAt from the slash-form id" in
    assert(
      extractor.isShownAt(rawFootage) === Seq(
        stringOnlyWebResource("https://americanarchive.org/catalog/cpb-aacip_513-000000145w")
      )
    )

  it should "construct isShownAt from the hyphen-form id" in
    assert(
      extractor.isShownAt(notOnline) === Seq(
        stringOnlyWebResource("https://americanarchive.org/catalog/cpb-aacip_37-95j9krh1")
      )
    )

  // preview — thumbnail only for Online Reading Room records

  it should "emit an S3 thumbnail preview for Online Reading Room records" in
    assert(
      extractor.preview(rawFootage) === Seq(
        stringOnlyWebResource(
          "https://s3.amazonaws.com/americanarchive.org/thumbnail/cpb-aacip-513-000000145w.jpg"
        )
      )
    )

  it should "emit no preview for non-Online-Reading-Room records" in
    assert(extractor.preview(notOnline).isEmpty)

  // Rights

  it should "map rightsSummary free text to rights" in
    assert(extractor.rights(rawFootage) === Seq("In Copyright"))

  it should "have no rights when the record carries no rightsSummary" in
    assert(extractor.rights(footrace).isEmpty)

  it should "map rightsLink URI to edmRights" in {
    val xml: Document[NodeSeq] = Document(
      <pbcoreDescriptionDocument>
        <pbcoreRightsSummary>
          <rightsSummary>In Copyright</rightsSummary>
          <rightsLink>http://rightsstatements.org/vocab/InC/1.0/</rightsLink>
        </pbcoreRightsSummary>
      </pbcoreDescriptionDocument>
    )
    assert(
      extractor.edmRights(xml) === Seq(URI("http://rightsstatements.org/vocab/InC/1.0/"))
    )
  }

  // Agents

  it should "map creators (name only) from pbcoreCreator/creator" in
    assert(
      extractor.creator(footrace) ===
        Seq(nameOnlyAgent("Bigbee, Dan"), nameOnlyAgent("Shangreaux, Lilly"))
    )

  it should "capture creator exactMatch from @ref" in
    assert(
      extractor.creator(episode) === Seq(
        EdmAgent(
          name = Some("WGBH Educational Foundation"),
          exactMatch = Seq(URI("http://id.loc.gov/authorities/names/n50052713"))
        )
      )
    )

  it should "map contributors from pbcoreContributor/contributor" in
    assert(extractor.contributor(episode).flatMap(_.name).contains("Roosevelt, Eleanor"))

  it should "map the top-level organization annotation to dataProvider" in {
    assert(extractor.dataProvider(rawFootage) === Seq(nameOnlyAgent("University of Houston")))
    assert(extractor.dataProvider(notOnline) === Seq(nameOnlyAgent("Iowa Public Television")))
  }

  // Subjects / genres

  it should "split ;-delimited subjects into separate concepts" in {
    val labels = extractor.subject(rawFootage).flatMap(_.providedLabel)
    assert(labels.contains("University of Houston"))
    assert(labels.contains("Football"))
    assert(labels.contains("Astrodome"))
    assert(labels.contains("Houston, Texas"))
  }

  it should "route AAPB Topical Genre to subject and AAPB Format Genre to genre" in {
    val subjects = extractor.subject(episode).flatMap(_.providedLabel)
    val genres = extractor.genre(episode).flatMap(_.providedLabel)
    assert(subjects.contains("Social Issues"))
    assert(subjects.contains("Politics and Government"))
    assert(genres.contains("Talk Show"))
    assert(!genres.contains("Social Issues"))
  }

  // Dates

  it should "strip 00 month/day padding from dates" in
    assert(extractor.date(footrace) === Seq(stringOnlyTimeSpan("2002")))

  it should "keep fully specified dates" in
    assert(extractor.date(rawFootage) === Seq(stringOnlyTimeSpan("1977-10-01")))

  // Type / format / extent / language

  it should "map instantiationMediaType to type" in
    assert(extractor.`type`(rawFootage) === Seq("Moving Image"))

  it should "map pbcoreAssetType to format" in
    assert(extractor.format(footrace) === Seq("Program"))

  it should "map instantiationDuration to extent" in
    assert(extractor.extent(rawFootage) === Seq("0:16:38"))

  it should "map essence track / instantiation language" in
    assert(extractor.language(episode).flatMap(_.providedLabel).contains("eng"))

  // Coverage -> place / temporal (inline; not present in the fixtures)

  it should "map Spatial coverage to place with @ref exactMatch" in {
    val xml: Document[NodeSeq] = Document(
      <pbcoreDescriptionDocument>
        <pbcoreCoverage>
          <coverage ref="https://www.wikidata.org/wiki/Q232" source="Wikidata">Kazakhstan</coverage>
          <coverageType>Spatial</coverageType>
        </pbcoreCoverage>
        <pbcoreCoverage>
          <coverage>1607-1631</coverage>
          <coverageType>Temporal</coverageType>
        </pbcoreCoverage>
      </pbcoreDescriptionDocument>
    )
    assert(
      extractor.place(xml) === Seq(
        DplaPlace(name = Some("Kazakhstan"), exactMatch = Seq(URI("https://www.wikidata.org/wiki/Q232")))
      )
    )
    assert(extractor.temporal(xml) === Seq(stringOnlyTimeSpan("1607-1631")))
  }

  it should "anchor to pbcoreDescriptionDocument nested inside a pbcoreCollection" in {
    val xml: Document[NodeSeq] = Document(
      <pbcoreCollection>
        <pbcoreDescriptionDocument>
          <pbcoreIdentifier source="http://americanarchiveinventory.org">cpb-aacip/111-222</pbcoreIdentifier>
          <pbcoreTitle titleType="Title">Nested Title</pbcoreTitle>
        </pbcoreDescriptionDocument>
      </pbcoreCollection>
    )
    assert(extractor.originalId(xml) === Some("cpb-aacip/111-222"))
    assert(extractor.title(xml) === Seq("Nested Title"))
  }
}
