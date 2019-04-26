package dpla.ingestion3.mappers.providers


import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.xml.{NodeSeq, XML}

class InMappingTest extends FlatSpec with BeforeAndAfter {

  val shortName = "in"
  val xmlString: String = new FlatFileIO().readFileAsString("/in.xml")
  val xml: Document[NodeSeq] = Document(XML.loadString(xmlString))
  val extractor = new InMapping

  it should "extract the correct originalId" in {
    val expected = Some("oai:dpla.library.in.gov:PALNI_herbarium4:oai:digital.library.in.gov:PALNI_herbarium4-22274")
    assert(extractor.originalId(xml) == expected)
  }

  it should "extract the correct dataProvider" in {
    val expected = Seq("Butler University").map(nameOnlyAgent)
    assert(extractor.dataProvider(xml) == expected)
  }

  it should "extract the correct edmRights" in {
    val expected = Seq("http://rightsstatements.org/vocab/InC/1.0/").map(URI)
    assert(extractor.edmRights(xml) == expected)
  }

  it should "map accessRight to edmRights if accessRight begins with 'http'" in {
    val xml =
      <record>
        <metadata>
          <oai_qdc:qualifieddc>
            <dcterms:accessRights>http://rightsstatements.org/vocab/InC/1.0/</dcterms:accessRights>
          </oai_qdc:qualifieddc>
        </metadata>
      </record>

    val expected = Seq("http://rightsstatements.org/vocab/InC/1.0/").map(URI)
    assert(expected === extractor.edmRights(Document(xml)))
  }

  it should "strip semicolons from the end of edmRights" in {
    val xml =
      <record>
        <metadata>
          <oai_qdc:qualifieddc>
            <dcterms:accessRights>http://rightsstatements.org/vocab/InC/1.0;</dcterms:accessRights>
          </oai_qdc:qualifieddc>
        </metadata>
      </record>

    val expected = Seq("http://rightsstatements.org/vocab/InC/1.0").map(URI)
    assert(expected === extractor.edmRights(Document(xml)))
  }

  it should "extract the correct intermediateProvider" in {
    val expected = Some("IUPUI (Campus). University Library").map(nameOnlyAgent)
    assert(extractor.intermediateProvider(xml) == expected)
  }

  it should "extract the correct isShownAt" in {
    val expected = Seq("http://palni.contentdm.oclc.org/cdm/ref/collection/herbarium4/id/22274")
      .map(stringOnlyWebResource)
    assert(extractor.isShownAt(xml) == expected)
  }

  it should "extract the correct alternate title" in {
    val expected = Seq("Heuchera villosa Michx.")
    assert(extractor.alternateTitle(xml) == expected)
  }

  it should "extract the correct contributor" in {
    val expected = Seq("Name of contributor").map(nameOnlyAgent)
    assert(extractor.contributor(xml) == expected)
  }

  it should "extract the correct creator" in {
    val expected = Seq("Ray C. Friesner", "Sallie", "Ron", "Sterling").map(nameOnlyAgent)
    assert(extractor.creator(xml) == expected)
  }

  it should "extract the correct date" in {
    val expected = Seq("1881", "1882").map(stringOnlyTimeSpan)
    assert(extractor.date(xml) == expected)
  }

  it should "extract the correct description" in {
    val expected = Seq("Tobacco landing")
    assert(extractor.description(xml) == expected)
  }

  it should "extract the correct extent" in {
    val expected = Seq("1 photograph : color")
    assert(extractor.extent(xml) == expected)
  }

  it should "extract the correct format" in {
    val expected = Seq("Compact cassette", "Documents", "Typed text")
    assert(extractor.format(xml) == expected)
  }

  it should "map type to format if type is not a valid DPLA type" in {
    val xml =
      <record>
        <metadata>
          <oai_qdc:qualifieddc>
            <dc:type>photograph</dc:type>
          </oai_qdc:qualifieddc>
        </metadata>
      </record>

    val expected = Seq("photograph")
    assert(expected === extractor.format(Document(xml)))
  }

  it should "extract the correct identifier" in {
    val expected = Seq("http://palni.contentdm.oclc.org/cdm/ref/collection/herbarium4/id/22274")
    assert(extractor.identifier(xml) == expected)
  }

  it should "extract the correct language" in {
    val expected = Seq("English", "Czech").map(nameOnlyConcept)
    assert(extractor.language(xml) == expected)
  }

  it should "extract the correct place" in {
    val expected = Seq("Muncie", "Delaware County", "Indiana", "United States", "North and Central America").map(nameOnlyPlace)
    assert(extractor.place(xml) == expected)
  }

  it should "extract the correct rights" in {
    val expected = Seq("Manchester College is providing access to these materials for educational and research purposes.")
    assert(extractor.rights(xml) == expected)
  }

  it should "filter out rights beginning with 'http'" in {
    val xml =
      <record>
        <metadata>
          <oai_qdc:qualifieddc>
            <dcterms:accessRights>http://rightsstatements.org/vocab/InC/1.0/</dcterms:accessRights>
          </oai_qdc:qualifieddc>
        </metadata>
      </record>

    val expected = Seq()
    assert(expected === extractor.rights(Document(xml)))
  }

  it should "extract the correct subject" in {
    val expected = Seq("Civil War, 1861-1865", "United States History", "Diaries").map(nameOnlyConcept)
    assert(extractor.subject(xml) == expected)
  }

  it should "extract the correct temporal" in {
    val expected = Seq("2000s (2000-2009)", "Twenty-first century, C. E.").map(stringOnlyTimeSpan)
    assert(extractor.temporal(xml) == expected)
  }

  it should "extract the correct title" in {
    val expected = Seq("Heuchera villosa")
    assert(extractor.title(xml) == expected)
  }

  it should "extract the correct type" in {
    val expected = Seq("Sound", "Text")
    assert(extractor.`type`(xml) == expected)
  }

  it should "extract the correct publisher" in {
    val expected = Seq("Lewis G. Hall, Jr.", "Dexter Press").map(nameOnlyAgent)
    assert(extractor.publisher(xml) == expected)
  }

  it should "extract the correct preview" in {
    val expected = Seq("http://palni.contentdm.oclc.org/utils/getthumbnail/collection/herbarium4/id/22274")
      .map(stringOnlyWebResource)
    assert(extractor.preview(xml) == expected)
  }
}
