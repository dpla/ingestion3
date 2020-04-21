package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.messages.{IngestMessage, MessageCollector}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.xml.{NodeSeq, XML}


class TxMappingTest extends FlatSpec with BeforeAndAfter {

  implicit val msgCollector: MessageCollector[IngestMessage] = new MessageCollector[IngestMessage]
  val shortName = "texas"
  val xmlString: String = new FlatFileIO().readFileAsString("/tx.xml")
  val xml: Document[NodeSeq] = Document(XML.loadString(xmlString))
  val extractor = new TxMapping

  it should "use the provider shortname in minting IDs " in
    assert(extractor.useProviderName)

  it should "extract the correct contributor when <name> present" in {
    val expected = Seq("Contributor").map(nameOnlyAgent)
    assert(extractor.contributor(xml) === expected)
  }

  it should "extract the correct contributor prop has text value" in {
    val xml =
      <record>
        <metadata>
          <untl:metadata xmlns:untl="http://digital2.library.unt.edu/untl/">
            <untl:contributor>Contributor text</untl:contributor>
          </untl:metadata>
        </metadata>
      </record>

    val expected = Seq("Contributor text").map(nameOnlyAgent)
    assert(extractor.contributor(Document(xml)) === expected)
  }

  it should "extract the correct creator" in {
    val expected = Seq("Adams, Charles Francis").map(nameOnlyAgent)
    assert(extractor.creator(xml) === expected)
  }

  it should "extract the correct description" in {
    val expected = Seq("Pamphlet containing an essay against the annexation of Texas, compiled from a series of newspaper columns.",
    "54 p.")
    assert(extractor.description(xml) === expected)
  }

//  it should "extract the correct format" in {
//    val expected = Seq("cloth")
//    assert(extractor.format(xml) === expected)
//  }

//  it should "extract the correct identifier" in {
//    val expected = Seq("1991.0076.0102")
//    assert(extractor.identifier(xml) === expected)
//  }

  it should "extract the correct original identifier " in
    assert(extractor.originalId(xml) === Some("info:ark/67531/metapth2355"))

  it should "extract the correct isShownAt value" in {
    val expected = Seq("https://texashistory.unt.edu/ark:/67531/metapth2355/")
      .map(stringOnlyWebResource)
    assert(extractor.isShownAt(xml) === expected)
  }

  it should "extract the correct language" in {
    val expected = Seq("eng").map(nameOnlyConcept)
    assert(extractor.language(xml) === expected)
  }

  it should "extract the correct place " in {
    val expected = Seq("United States", "United States - Texas").map(nameOnlyPlace)
    assert(extractor.place(xml) === expected)
  }

  it should "extract the correct preview values" in {
    val expected = Seq(
      "https://texashistory.unt.edu/ark:/67531/metapth2355/small/")
      .map(stringOnlyWebResource)
    assert(extractor.preview(xml) === expected)
  }

  it should "extract the correct publisher" in {
    val expected = Seq("Boston, Massachusetts: Eastburn's Press").map(nameOnlyAgent)
    assert(extractor.publisher(xml) === expected)
  }

  it should "extract the correct rights statement" in {
    val expected = Seq("License: Attribution.", "The contents of The Portal to Texas History (digital content including images, text, and sound and video recordings) are made publicly available by the collection-holding partners for use in research, teaching, and private study. For the full terms of use, see https://texashistory.unt.edu/terms-of-use/")
    assert(extractor.rights(xml) === expected)
  }

  it should "extract the correct subjects" in {
    val expected = Seq("Texas -- Annexation to the United States.",
      "United States -- Politics and government -- 1841-1845.",
      "Military and War - Wars - Texas Revolution").map(nameOnlyConcept)
    assert(extractor.subject(xml) === expected)
  }

  it should "extract the correct titles" in {
    val expected = Seq("Texas and the Massachusetts Resolutions")
    assert(extractor.title(xml) === expected)
  }

  it should "extract the correct types" in {
    val expected = Seq("text") // these will get cleaned up by type enrichment
    assert(extractor.`type`(xml) === expected)
  }

  it should "extract the correct dataProvider" in {
    val expected = Seq("UNT Libraries").map(nameOnlyAgent)
    assert(extractor.dataProvider(xml) === expected)
  }

  it should "extract the correct RS.org value to edmRights" in {
    // <untl:rights qualifier="statement">http://rightsstatements.org/vocab/NoC-US/1.0/</untl:rights>
    val xml =
      <record>
        <metadata>
          <untl:metadata xmlns:untl="http://digital2.library.unt.edu/untl/">
            <untl:rights qualifier="statement">http://rightsstatements.org/vocab/NoC-US/1.0/</untl:rights>
          </untl:metadata>
        </metadata>
      </record>

    val expected = Seq(URI("http://rightsstatements.org/vocab/NoC-US/1.0/"))
    assert(extractor.edmRights(Document(xml)) === expected)
  }

  it should "extract the correct CC.org value to edmRights" in {
    // <untl:rights qualifier="license">https://creativecommons.org/licenses/by/4.0/</untl:rights>
    val xml =
      <record>
        <metadata>
          <untl:metadata xmlns:untl="http://digital2.library.unt.edu/untl/">
            <untl:rights qualifier="license">https://creativecommons.org/licenses/by/4.0/</untl:rights>
          </untl:metadata>
        </metadata>
      </record>

    val expected = Seq(URI("https://creativecommons.org/licenses/by/4.0/"))
    assert(extractor.edmRights(Document(xml)) === expected)
  }
}
