package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.messages.{IngestMessage, MessageCollector}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.xml.{NodeSeq, XML}

class OrbisCascadeMappingTest extends FlatSpec with BeforeAndAfter {

  implicit val msgCollector: MessageCollector[IngestMessage] = new MessageCollector[IngestMessage]
  val shortName = "orbis-cascade"
  val xmlString: String = new FlatFileIO().readFileAsString("/orbis-cascade.xml")
  val xml: Document[NodeSeq] = Document(XML.loadString(xmlString))
  val extractor = new OrbisCascadeMapping

  it should "use the provider shortname in minting IDs "in
    assert(extractor.useProviderName())

  it should "extract the correct original identifier " in
    assert(extractor.originalId(xml) === Some("oai:digital.lib.uidaho.edu:psychiana/0"))

  it should "extract the correct creators" in {
    val expected = Seq("Robinson, Frank B.").map(nameOnlyAgent)
    assert(extractor.creator(xml) == expected)
  }

  it should "extract the correct dates" in {
    val expected = Seq("1946").map(stringOnlyTimeSpan)
    assert(extractor.date(xml) === expected)
  }

  it should "extract the correct description" in
    assert(extractor.description(xml) == Seq("A description"))

  it should "extract the correct identifier" in {
    val expected = Seq("MG101_b5_f1_001")
    assert(extractor.identifier(xml) === expected)
  }

  it should "extract the correct language" in {
    val expected = Seq("eng").map(nameOnlyConcept)
    assert(extractor.language(xml) === expected)
  }

  it should "extract the correct publisher" in {
    val expected = Seq("University of Idaho Library Digital Initiatives, http://www.lib.uidaho.edu/digital/").map(nameOnlyAgent)
    assert(extractor.publisher(xml) === expected)
  }

  it should "extract edmRights value if it is a URI'" in {
    val rightsXml: Document[NodeSeq] = Document(<record xmlns:dc="http://purl.org/dc/elements/1.1/">
      <metadata>
        <oai_dc:dc>
          <dc:rights>https://rightsstatements.org/vocab/Inc/1.0/</dc:rights>
        </oai_dc:dc>
      </metadata>
    </record>)
    val expected = Seq(URI("https://rightsstatements.org/vocab/Inc/1.0/"))
    assert(extractor.edmRights(rightsXml) === expected)
  }

  it should "extract rights value it is text'" in {
    val rightsXml: Document[NodeSeq] = Document(<record xmlns:dc="http://purl.org/dc/elements/1.1/">
      <metadata>
        <oai_dc:dc>
          <dc:rights>rights statement</dc:rights>
        </oai_dc:dc>
      </metadata>
    </record>)
    val expected = Seq("rights statement")
    assert(extractor.rights(rightsXml) === expected)
  }

  it should "extract the correct subjects" in {
    val expected = Seq("God-Power; Psychiana Criticism; Psychiana Origins; God on Earth").map(nameOnlyConcept)
    assert(extractor.subject(xml) === expected)
  }

  it should "extract the correct titles" in {
    val expected = Seq("We Read an Amazing Story, Chapter 4 with Editors Corrections 2")
    assert(extractor.title(xml) === expected)
  }

  it should "extract the correct types" in {
    val expected = Seq("Text")
    assert(extractor.`type`(xml) === expected)
  }

  it should "extract the correct edmRights" in {
    val expected = Seq(URI("http://rightsstatements.org/vocab/InC-EDU/1.0/"))
    assert(extractor.edmRights(xml) === expected)
  }
}