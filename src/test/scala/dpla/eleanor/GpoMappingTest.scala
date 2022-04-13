package dpla.eleanor

import dpla.eleanor.mappers.GpoMapping
import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.model.{nameOnlyAgent, nameOnlyConcept, stringOnlyWebResource}
import org.apache.commons.io.IOUtils
import org.scalatest.FlatSpec

import scala.io.Source
import scala.xml.{Elem, NodeSeq, XML}

class GpoMappingTest extends FlatSpec {
  val mapping = new GpoMapping

  /**
    * Reads a file and returns it as a single string
    * @param name
    * @return
    */
  def readFileAsString(name: String): String = {
    val stream = getClass.getResourceAsStream(name)
    val result = Source.fromInputStream(stream).mkString
    IOUtils.closeQuietly(stream)
    result
  }

  val xmlString: String = readFileAsString("/gpo_ebooks.xml")
  val xml: Elem = XML.loadString(xmlString)
  val doc: Document[NodeSeq] = Document(xml)

  it should "map the correct title" in {
    val expected = Seq("Physiological, subjective, and performance correlates of reported boredom and monotony while performing a simulated radar control task /")
    assert(mapping.title(doc) === expected)
  }

  it should "map the correct creator" in {
    val expected = Seq(nameOnlyAgent("Thackray, Richard I."))
    assert(mapping.creator(doc) === expected)
  }
  it should "extract the correct language (code)" in {
    val xml =
      <record>
        <metadata>
          <marc:record>
            <marc:datafield ind2=" " ind1="0" tag="041">
              <marc:subfield code="a">eng</marc:subfield>
              <marc:subfield code="a">spa</marc:subfield>
            </marc:datafield>
          </marc:record>
        </metadata>
      </record>
      val expected = Seq(nameOnlyConcept("eng"), nameOnlyConcept("spa"))
    assert(mapping.language(Document(xml)) === expected)
  }

  it should "map the correct description" in {
    val expected = Seq("Cover title.", "\"FAA-AM-75-8.\"", "Prepared by Civil Aeromedical Institute, Oklahoma City, under task AM-C-75-PSY-48.", "[pbk. $3.00, mf $0.95, 7 cds]", "Also available via Internet from the FAA web site. Address as of 5/15/07: http://www.faa.gov/library/reports/medical/oamtechreports/1970s/media/AM75-08.pdf; current access is available via PURL.", "Includes bibliographical references (p. 9).")
    assert(mapping.description(doc) === expected)
  }

  it should "extract the correct isShownAt" in {
    val expected = Seq(stringOnlyWebResource("http://catalog.gpo.gov/F/?func=direct&doc_number=000003356&format=999"))
    assert(mapping.isShownAt(doc) === expected)
  }
}
