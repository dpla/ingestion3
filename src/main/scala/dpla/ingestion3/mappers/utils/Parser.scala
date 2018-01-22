package dpla.ingestion3.mappers.utils

import org.json4s.JValue

import scala.xml.{NodeSeq, XML}

/**
  * Parses original records
  *
  * @tparam T
  */
trait Parser[T] {
  def parse(data: String): T
}

/**
  * XML parser
  */
class XmlParser extends Parser[NodeSeq] {
  override def parse(data: String): NodeSeq = XML.loadString(data)
}

/**
  * JSON parser
  */
class JsonParser extends Parser[JValue] {
  override def parse(data: String): JValue = org.json4s.jackson.JsonMethods.parse(data)
}
