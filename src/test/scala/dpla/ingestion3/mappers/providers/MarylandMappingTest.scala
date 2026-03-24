package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.messages.{IngestMessage, MessageCollector}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

import scala.xml.{NodeSeq, XML}

class MarylandMappingTest extends AnyFlatSpec with BeforeAndAfter {

  implicit val msgCollector: MessageCollector[IngestMessage] = new MessageCollector[IngestMessage]
  val shortName = "maine"
  val xmlString: String = new FlatFileIO().readFileAsString("/maryland.xml")
  val xml: Document[NodeSeq] = Document(XML.loadString(xmlString))
  val extractor = new MarylandMapping

  // Record with only a URL identifier (no local ID prefix) — tests isShownAt URL filter
  val xmlSingleIdentifier: Document[NodeSeq] = Document(XML.loadString(
    """<record xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns="http://www.openarchives.org/OAI/2.0/">
      |  <header><identifier>oai:collections.digitalmaryland.org:acfp/11016</identifier></header>
      |  <metadata>
      |    <oai_qdc:qualifieddc xmlns:oai_qdc="http://worldcat.org/xmlschemas/qdc-1.0/"
      |        xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/">
      |      <dc:title>Cover</dc:title>
      |      <dc:source>Enoch Pratt Free Library</dc:source>
      |      <dc:rights>https://rightsstatements.org/vocab/CNE/1.0/</dc:rights>
      |      <dc:identifier>http://collections.digitalmaryland.org/cdm/ref/collection/acfp/id/11016</dc:identifier>
      |    </oai_qdc:qualifieddc>
      |  </metadata>
      |</record>""".stripMargin))

  // Record with no dc:rights but dcterms:accessRights containing CC BY-ND 3.0 text
  val xmlAccessRightsOnly: Document[NodeSeq] = Document(XML.loadString(
    """<record xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns="http://www.openarchives.org/OAI/2.0/">
      |  <header><identifier>oai:collections.digitalmaryland.org:msa/1</identifier></header>
      |  <metadata>
      |    <oai_qdc:qualifieddc xmlns:oai_qdc="http://worldcat.org/xmlschemas/qdc-1.0/"
      |        xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/">
      |      <dc:title>Test Item</dc:title>
      |      <dc:identifier>msa_local_001</dc:identifier>
      |      <dc:identifier>http://collections.digitalmaryland.org/cdm/ref/collection/msa/id/1</dc:identifier>
      |      <dcterms:accessRights>This work is licensed under a Creative Commons Attribution-NoDerivs 3.0 Unported License. When this material is used, proper citation must be attributed to the Maryland State Archives.</dcterms:accessRights>
      |    </oai_qdc:qualifieddc>
      |  </metadata>
      |</record>""".stripMargin))

  it should "use the provider shortname in minting IDs " in
    assert(extractor.useProviderName)

  it should "extract the correct original identifier " in {
    val expected = Some("oai:collections.digitalmaryland.org:mamo/29817")
    assert(extractor.originalId(xml) === expected)
  }

  it should "extract the correct contributor" in {
    val expected = Seq("Hayden, Edwin Parsons, 1811-1850", "Jessop, Charles", "Jessop, William", "Shipley, Nathan")
      .map(nameOnlyAgent)
    assert(extractor.contributor(xml) == expected)
  }

  it should "extract the correct creator" in {
    val expected = Seq("O'Dell, Mark").map(nameOnlyAgent)
    assert(extractor.creator(xml) == expected)
  }

  it should "extract the correct date" in {
    val expected = Seq("2008-10-21").map(stringOnlyTimeSpan)
    assert(extractor.date(xml) === expected)
  }

  it should "extract the correct description" in {
    val expected = Seq("Photograph of Lieutenant Governor Anthony Brown at the Winning with Asthma Program Kick Off Event on October 21, 2008.")
    assert(extractor.description(xml) == expected)
  }

  it should "extract the correct format" in {
    val expected = Seq("Color digital photograph/jpeg ")
    assert(extractor.format(xml) == expected)
  }

  it should "extract the correct language" in {
    val expected = Seq("English").map(nameOnlyConcept)
    assert(extractor.language(xml) == expected)
  }

  it should "extract the correct publisher" in {
    val expected = Seq("Alfred A. Knopf, Inc.").map(nameOnlyAgent)
    assert(extractor.publisher(xml) === expected)
  }

  it should "extract the correct subjects" in {
    val expected = Seq("Brown, Anthony G., 1961-", "Governors--Maryland", "Maryland--Politics and government", "Asthma")
      .map(nameOnlyConcept)
    assert(extractor.subject(xml) === expected)
  }

  it should "extract the correct temporal" in {
    val expected = Seq("1970-1979").map(stringOnlyTimeSpan)
    assert(extractor.temporal(xml) === expected)
  }

  it should "extract the correct title" in {
    val expected = Seq("Lieutenant Governor Anthony Brown at the Winning with Asthma Program Kick Off Event")
    assert(extractor.title(xml) === expected)
  }

  it should "extract the correct type" in {
    val expected = Seq("Image")
    assert(extractor.`type`(xml) === expected)
  }

  it should "extract the correct dataProvider" in {
    val expected = Seq(nameOnlyAgent("Maryland State Archives"))
    assert(extractor.dataProvider(xml) === expected)
  }

  it should "extract the correct edmRights" in {
    val expected = Seq(URI("http://rightsstatements.org/vocab/NoC-OKLR/1.0/ "))
    assert(extractor.edmRights(xml) === expected)
  }

  it should "extract the correct isShownAt" in {
    val expected = Seq(stringOnlyWebResource("http://collections.digitalmaryland.org/cdm/ref/collection/mamo/id/29817"))
    assert(extractor.isShownAt(xml) === expected)
  }

  it should "extract the correct preview" in {
    val expected = Seq(stringOnlyWebResource("http://webconfig.digitalmaryland.org/utils/getthumbnail/collection/mamo/id/29817"))
    assert(extractor.preview(xml) === expected)
  }

  it should "create the correct DPLA URI" in {
    val expected = Some(URI("http://dp.la/api/items/5db1d7b6c61b021fadcffdca899a4d69"))
    assert(extractor.dplaUri(xml) === expected)
  }

  it should "extract rights text from dcterms:accessRights" in {
    val expected = Seq("This work is licensed under a Creative Commons Attribution-NoDerivs 3.0 Unported License. When this material is used, in whole or in part, proper citation and credit must be attributed to the Maryland State Archives. For more information go to http://histpics.msa.maryland.gov/pages/Search.aspx ")
    assert(extractor.rights(xml) === expected)
  }

  // isShownAt: single URL identifier (no local ID prefix)
  it should "extract isShownAt when only a URL identifier is present" in {
    val expected = Seq(stringOnlyWebResource("http://collections.digitalmaryland.org/cdm/ref/collection/acfp/id/11016"))
    assert(extractor.isShownAt(xmlSingleIdentifier) === expected)
  }

  // edmRights: falls back to CC URI detected from accessRights text when dc:rights is absent
  it should "extract edmRights CC URI from dcterms:accessRights when dc:rights is absent" in {
    val expected = Seq(URI("https://creativecommons.org/licenses/by-nd/3.0/"))
    assert(extractor.edmRights(xmlAccessRightsOnly) === expected)
  }

  // rights: falls back to accessRights text when dc:rights is absent
  it should "extract rights text from dcterms:accessRights when dc:rights is absent" in {
    val expected = Seq("This work is licensed under a Creative Commons Attribution-NoDerivs 3.0 Unported License. When this material is used, proper citation must be attributed to the Maryland State Archives.")
    assert(extractor.rights(xmlAccessRightsOnly) === expected)
  }
}
