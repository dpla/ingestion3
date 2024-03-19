package dpla.ingestion3.mappers.utils

import org.json4s.JValue
import scala.xml.{NodeSeq, XML}

trait Parser[T] {
  def parse(data: String): Document[T]
}

class XmlParser extends Parser[NodeSeq] {
  override def parse(data: String): Document[NodeSeq] = Document(XML.loadString(data))
}


class JsonParser extends Parser[JValue] {
  override def parse(data: String): Document[JValue] = Document(org.json4s.native.JsonMethods.parse(data))
}
