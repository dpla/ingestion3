package dpla.eleanor

import dpla.eleanor.mappers.GpoMapping
import org.apache.commons.io.IOUtils
import org.scalatest.FlatSpec

import scala.io.Source
import scala.xml.{Elem, XML}

class GpoMappingTest extends FlatSpec {

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

  it should "map the correct title" in {
    val expected = Seq("Physiological, subjective, and performance correlates of reported boredom and monotony while performing a simulated radar control task /")
    assert(GpoMapping.title(xml) === expected)
  }

  it should "map the correct author" in {
    val expected = Seq("Thackray, Richard I.")
    assert(GpoMapping.author(xml) === expected)
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
      val expected = Seq("eng", "spa")
    assert(GpoMapping.language(xml) === expected)
  }

  it should "map the correct summary" in {
    val expected = Seq("Cover title.", "\"FAA-AM-75-8.\"", "Prepared by Civil Aeromedical Institute, Oklahoma City, under task AM-C-75-PSY-48.", "[pbk. $3.00, mf $0.95, 7 cds]", "Also available via Internet from the FAA web site. Address as of 5/15/07: http://www.faa.gov/library/reports/medical/oamtechreports/1970s/media/AM75-08.pdf; current access is available via PURL.", "Includes bibliographical references (p. 9).")
    assert(GpoMapping.summary(xml) === expected)
  }

  it should "extract the correct uri" in {
    val expected = Seq("http://catalog.gpo.gov/F/?func=direct&doc_number=000003356&format=999")
    assert(GpoMapping.uri(xml) === expected)
  }
}
