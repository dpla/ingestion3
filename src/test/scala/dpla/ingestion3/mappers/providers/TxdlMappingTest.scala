package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.messages.{IngestMessage, MessageCollector}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

import scala.xml.{NodeSeq, XML}


class TxdlMappingTest extends AnyFlatSpec with BeforeAndAfter {

  implicit val msgCollector: MessageCollector[IngestMessage] = new MessageCollector[IngestMessage]
  val shortName = "texas"
  val xmlString: String = new FlatFileIO().readFileAsString("/txdl.xml")
  val xml: Document[NodeSeq] = Document(XML.loadString(xmlString))
  val extractor = new TxdlMapping

  it should "use the provider shortname in minting IDs " in
    assert(extractor.useProviderName)

  it should "extract the correct originalId " in {
    val expected = Some("oai:cdm17006.contentdm.oclc.org:p17006coll17/0")
    assert(extractor.originalId(xml) === expected)
  }

  it should "extract the correct alternate title" in {
    val expected = Seq("alt title")
    assert(extractor.alternateTitle(xml) === expected)
  }

  it should "extract the correct collection title" in {
    val expected = Seq("collection title").map(nameOnlyCollection)
    assert(extractor.collection(xml) === expected)
  }

  it should "extract the correct contributor" in {
    val expected = Seq("Texas Archive of the Moving Image available at: http://www.texasarchive.org/").map(nameOnlyAgent)
    assert(extractor.contributor(xml) === expected)
  }

  it should "extract the correct creator" in {
    val expected = Seq("creator").map(nameOnlyAgent)
    assert(extractor.creator(xml) === expected)
  }

  it should "extract the correct date" in {
    val expected = Seq("1952").map(stringOnlyTimeSpan)
    assert(extractor.date(xml) === expected)
  }

  it should "extract the correct description" in {
    val expected = Seq("description")
    assert(extractor.description(xml) === expected)
  }

  it should "extract the correct format" in {
    val expected = Seq("mp4; 29 frames/second; sound and no sound; black and white; color; 4 : 3 aspect ratio")
    assert(extractor.format(xml) === expected)
  }

  it should "extract the correct identifier" in {
    val expected = Seq("MSS0377-r0001")
    assert(extractor.identifier(xml) === expected)
  }

  it should "extract the correct isShownAt value" in {
    val expected = Seq("http://cdm17006.contentdm.oclc.org/cdm/ref/collection/p17006coll17/id/0").map(stringOnlyWebResource)
    assert(extractor.isShownAt(xml) === expected)
  }

  it should "extract the correct language" in {
    val expected = Seq("English").map(nameOnlyConcept)
    assert(extractor.language(xml) === expected)
  }

  it should "extract the correct place " in {
    val expected = Seq("1950s").map(nameOnlyPlace)
    assert(extractor.place(xml) === expected)
  }

  it should "extract the correct preview values" in {
    val expected = Seq("http://cdm17006.contentdm.oclc.org/utils/getthumbnail/collection/p17006coll17/id/0")
      .map(stringOnlyWebResource)
    assert(extractor.preview(xml) === expected)
  }

  it should "extract the correct publisher" in {
    val expected = Seq("Electronic version published by Houston Public Library, Houston, Texas").map(nameOnlyAgent)
    assert(extractor.publisher(xml) === expected)
  }

  it should "extract the correct rights statement" in {
    val expected = Seq("Please contact the Houston Metropolitan Research Center, Houston, Texas.")
    assert(extractor.rights(xml) === expected)
  }

  it should "extract the correct edmrights " in {
    val expected = Seq(URI("http://rightsatement.org"))
    assert(extractor.edmRights(xml) === expected)
  }

  it should "extract the correct subjects" in {
    val expected = Seq("Rice University",
      "College students",
      "College administrators",
      "College teachers").map(nameOnlyConcept)
    assert(extractor.subject(xml) === expected)
  }

  it should "extract the correct title" in {
    val expected = Seq("Shad E. Graham papers Film Reel 1")
    assert(extractor.title(xml) === expected)
  }

  it should "extract the correct types" in {
    val expected = Seq("MovingImage") // these will get cleaned up by type enrichment
    assert(extractor.`type`(xml) === expected)
  }

  it should "extract the correct dataProvider" in {
    val expected = Seq("dataProvider").map(nameOnlyAgent)
    assert(extractor.dataProvider(xml) === expected)
  }
}
