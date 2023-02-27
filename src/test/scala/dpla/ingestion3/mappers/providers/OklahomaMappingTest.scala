package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.messages.{IngestMessage, MessageCollector}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.xml.{NodeSeq, XML}

class OklahomaMappingTest extends FlatSpec with BeforeAndAfter {

  implicit val msgCollector: MessageCollector[IngestMessage] = new MessageCollector[IngestMessage]
  val shortName = "oklahoma"
  val xmlString: String = new FlatFileIO().readFileAsString("/oklahoma.xml")
  val xml: Document[NodeSeq] = Document(XML.loadString(xmlString))
  val extractor = new OklahomaMapping

  it should "not use the provider shortname in minting IDs " in
    assert(extractor.useProviderName())

  it should "extract the correct original identifier" in {
    val expected = Some("oai:okhub:OKCES:oai:shareok.org:11244/50280")
    assert(extractor.originalId(xml) == expected)
  }

  it should "extract the correct alternate titles" in {
    val expected = Seq("Alt Title")
    assert(extractor.alternateTitle(xml) === expected)
  }

  it should "extract the correct collection titles" in {
    val expected = Seq("Oklahoma Cooperative Extension Service")
      .map(nameOnlyCollection)
    assert(extractor.collection(xml) === expected)
  }

  it should "extract the correct contributors" in {
    val expected = Seq("Oklahoma Cooperative Extension Service").map(nameOnlyAgent)
    assert(extractor.contributor(xml) == expected)
  }

  it should "extract the correct creators" in {
    val expected = Seq("Hillock, David A.", "McCraw, B. Dean").map(nameOnlyAgent)
    assert(extractor.creator(xml) == expected)
  }

  it should "extract the correct date" in {
    val expected = Seq("2007").map(stringOnlyTimeSpan)
    assert(extractor.date(xml) === expected)
  }

  it should "extract the correct description" in
    assert(extractor.description(xml) == Seq("The Oklahoma Cooperative Extension Service periodically issues revisions to its publications. The most current edition is made available. For access to an earlier edition, if available for this title, please contact the Oklahoma State University Library Archives by email at libscua@okstate.edu or by phone at 405-744-6311.", "Horticulture and Landscape Architecture"))

  it should "extract the correct extent" in {
    assert(extractor.extent(xml) == Seq("Extent"))
  }

  it should "extract the correct format" in {
    val expected = Seq("text", "newsletter", "Phys Desc Note")
    assert(extractor.format(xml) === expected)
  }

  it should "extract the correct identifiers" in {
    val expected = Seq("oksd_hla_6222_2007-03", "http://hdl.handle.net/11244/50280")
    assert(extractor.identifier(xml) === expected)
  }

  it should "extract the correct languages" in {
    val expected = Seq("en_US").map(nameOnlyConcept)
    assert(extractor.language(xml) === expected)
  }

  it should "extract the correct place" in {
    val expected = Seq("Oklahoma").map(nameOnlyPlace)
    assert(extractor.place(xml) === expected)
  }

  it should "extract the correct publisher" in {
    val expected = Seq("Oklahoma State University. Division of Agricultural Sciences and Natural Resources. Cooperative Extension Service").map(nameOnlyAgent)
    assert(extractor.publisher(xml) === expected)
  }

  it should "extract the correct relation" in {
    val expected = Seq("Oklahoma Cooperative Extension fact sheets ; HLA-6222")
      .map(eitherStringOrUri)
    assert(extractor.relation(xml) === expected)
  }

  it should "extract the correct subjects" in {
    val expected = Seq("fruit--planting time", "gardens--planning")
      .map(nameOnlyConcept)
    assert(extractor.subject(xml) === expected)
  }

  it should "extract the correct temporal" in {
    val expected = Seq("Temporal")
      .map(stringOnlyTimeSpan)
    assert(extractor.temporal(xml) === expected)
  }

  it should "extract the correct titles" in {
    val expected = Seq("Home Fruit Planting Guide", "Alt Title")
    assert(extractor.title(xml) === expected)
  }

  it should "extract the correct type" in {
    val expected = Seq("text")
    assert(extractor.`type`(xml) === expected)
  }

  it should "extract the correct dataProvider" in {
    val expected = Seq("Oklahoma State University Library").map(nameOnlyAgent)
    assert(extractor.dataProvider(xml) === expected)
  }

  it should "extract the correct isShownAt" in {
    val expected = Seq(uriOnlyWebResource(URI("http://hdl.handle.net/11244/50280")))
    assert(extractor.isShownAt(xml) === expected)
  }

  it should "extract the correct IIIF manifest" in {
    val expected = Seq(URI("http://hdl.handle.net/11244/50280"))
    assert(extractor.iiifManifest(xml) === expected)
  }

  it should "extract the correct preview" in {
    val expected = Seq(stringOnlyWebResource("http://img.preview/thumb"))
    assert(extractor.preview(xml) === expected)
  }

  it should "extract rights value" in {
    assert(extractor.rights(xml) === Seq("The researcher assumes full responsibility for conforming with the laws of copyright. Whenever possible, the Oklahoma State University Archives will provide information about copyright owners and related information. Securing permission to publish or use material is the responsibility of the researcher. Note that unless specifically transferred to Oklahoma State University Libraries, any applicable copyrights may be held by another individual or entity. Copyright for material published by Oklahoma Agricultural and Mechanical College/Oklahoma State University is held by the Board of Regents for the Oklahoma Agricultural and Mechanical Colleges. All rights reserved. Further information about copyright policy can be obtained by contacting the OSU Archives by email at libscua@okstate.edu or by phone at 405-744-6311."))
  }

  it should "extract the correct edmRights " in {
    val expected = Seq(URI("http://rightsstatements.org/vocab/InC-EDU/1.0/"))
    assert(extractor.edmRights(xml) === expected)
  }

  it should "create the correct DPLA URI" in {
    val expected = Some(URI("http://dp.la/api/items/b850987a7386bae1a31d703b6bb3e5f7"))
    assert(extractor.dplaUri(xml) === expected)
  }
}
