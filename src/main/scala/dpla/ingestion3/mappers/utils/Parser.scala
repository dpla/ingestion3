package dpla.ingestion3.mappers.utils

import org.json4s.JValue

import scala.xml.{NodeSeq, XML}

/**
  * Parses original records
  *
  * @tparam T
  */

/**
  * XML parser
  */
class XmlParser extends Parser[Document[NodeSeq]] {
  override def parse(data: String): Document[NodeSeq] = Document(XML.loadString(data))
}

/**
  * JSON parser
  */
class JsonParser extends Parser[JValue] {
  override def parse(data: String): JValue = org.json4s.jackson.JsonMethods.parse(data)
}
