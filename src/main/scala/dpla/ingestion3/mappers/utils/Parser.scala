package dpla.ingestion3.mappers.utils

import org.json4s.JValue
import scala.xml.{NodeSeq, XML}

/**
  * XML parser
  */
class XmlParser extends Parser[NodeSeq] {
  override def parse(data: String): Document[NodeSeq] = Document(XML.loadString(data))
}

/**
  * JSON parser
  */
class JsonParser extends Parser[JValue] {
  override def parse(data: String): Document[JValue] = Document(org.json4s.jackson.JsonMethods.parse(data))
}
