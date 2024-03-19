package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.messages.{IngestMessage, MessageCollector}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

import scala.xml.{NodeSeq, XML}

class EsdnMappingTest extends AnyFlatSpec with BeforeAndAfter {

  implicit val msgCollector: MessageCollector[IngestMessage] = new MessageCollector[IngestMessage]
  val shortName = "esdn"
  val xmlString: String = new FlatFileIO().readFileAsString("/esdn.xml")
  val xml: Document[NodeSeq] = Document(XML.loadString(xmlString))
  val extractor = new EsdnMapping

  it should "not use the provider shortname in minting IDs " in
    assert(extractor.useProviderName)

  it should "extract the correct original identifier " in
    assert(extractor.originalId(xml) === Some("oai:repox.ist.utl.pt:bklynsheetmusic_collection:oai:dcmny.org:bklynsheetmusic_55"))

  it should "extract the correct alternate titles" in {
    val expected = Seq("Alternate Title")
    assert(extractor.alternateTitle(xml) === expected)
  }

  it should "extract the correct collection titles" in {
    val expected = Seq("Collection Title")
      .map(nameOnlyCollection)
    assert(extractor.collection(xml) === expected)
  }

  it should "extract the correct contributors" in {
    val expected = Seq("Contributor").map(nameOnlyAgent)
    assert(extractor.contributor(xml) == expected)
  }

  it should "extract the correct creators" in {
    val expected = Seq("F. A. Cotharin", "Creator").map(nameOnlyAgent)
    assert(extractor.creator(xml) == expected)
  }

  it should "extract the correct dates when only given a keyDate" in {
    val xml: Document[NodeSeq] = Document(
      <record>
        <originInfo>
          <dateCreated keyDate="yes">2010-11-31</dateCreated>
        </originInfo>
      </record>)

    val expected = Seq("2010-11-31").map(stringOnlyTimeSpan)
    assert(extractor.date(xml) === expected)
  }

  it should "extract the correct dates when given point=start and point=end" in {
    val xml: Document[NodeSeq] = Document(
      <record>
        <originInfo>
          <dateCreated keyDate="yes" point="start">2010-11-01</dateCreated>
          <dateCreated point="end">2010-11-31</dateCreated>
        </originInfo>
      </record>)

    val expected = Seq(
      EdmTimeSpan(
        originalSourceDate = Some("2010-11-01-2010-11-31"),
        begin = Some("2010-11-01"),
        end = Some("2010-11-31")
      ))
    assert(extractor.date(xml) === expected)
  }

  it should "extract the correct description" in
    assert(extractor.description(xml) == Seq("Description"))

  it should "extract the correct extent" in {
    assert(extractor.extent(xml) == Seq("Extent"))
  }

  it should "extract the correct format" in {
    val expected = Seq("Form", "Sheet music covers", "Advertisements", "Genre")
    assert(extractor.format(xml) === expected)
  }

  it should "extract rights value" in {
    assert(extractor.rights(xml) === Seq("rights 1", "rights 2"))
  }

  it should "extract the correct edmRights " in {
    val expected = Seq(URI("http://rightsstatements.org/vocab/InC-EDU/1.0/"))
    assert(extractor.edmRights(xml) === expected)
  }

  it should "extract the correct titles" in {
    val expected = Seq("And a Genuine Gold Plated Chain and Charm complete, $8.00 [advertisement]")
    assert(extractor.title(xml) === expected)
  }

  it should "extract the correct isShownAt" in {
    val expected = Seq(uriOnlyWebResource(URI("http://isurl.com")))
    assert(extractor.isShownAt(xml) === expected)
  }

  it should "create the correct DPLA URI" in {
    val expected = Some(URI("http://dp.la/api/items/9a6660ca7d23f478cbc1b3fed534af72"))
    assert(extractor.dplaUri(xml) === expected)
  }
}
