package dpla.ingestion3.mappers.xml

import org.scalatest.FlatSpec
import scala.xml._

class XmlExtractionUtilsTest extends FlatSpec with XmlExtractionUtils {
  val xmlStr =
    """
      |<record>
      | <foo>bar</foo>
      | <foo>bur</foo>
      | <foo>ber</foo>
      | <pop>top</pop>
      | <subjects>
      |   <subject>Hills</subject>
      |   <subject>Trees</subject>
      | </subjects>
      |</record>
    """.stripMargin

  val xml = XML.loadString(xmlStr)

  "A XmlExtractionUtils" should "extract a single string from a field named by a string" in {
    val eString = extractString("pop")(xml)
    assert(eString.isDefined)
    assert(eString.get.contentEquals("top"))
  }

  it should "extract a Sequence of string values if there are multiple occurrences of the named field" in {
    val eStrings = extractStrings("foo")(xml)

    assert(eStrings.length == 3)
    assert(Seq("bar","bur","ber") == eStrings)
  }

  it should "extract all string values if there are multiple properties nested path" in {
    val eStrings = extractStrings("subject")(xml)
    assert(eStrings.length == 2)
    assert(Seq("Hills","Trees") == eStrings)
  }

  it should "extract an empty Seq() if there are no occurrences of the named field" in {
    assert(Seq() == extractStrings("Cap")(xml))
  }

  it should "extract None if there are no occurrences of the named field" in {
    assert(!extractString("Cap")(xml).isDefined)
  }
}
