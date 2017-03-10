package dpla.ingestion3.mappers.xml

import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import scala.xml.XML._

class XmlExtractionUtilsText extends FlatSpec with Matchers with BeforeAndAfter with XmlExtractionUtils {
  "A XmlExtractionUtils" should "extract strings from a field named by a string" in {
    val xml = xml.load("<foo>bar</foo>")
    extractString("foo")(xml) should be(Some("bar"))
  }
}
