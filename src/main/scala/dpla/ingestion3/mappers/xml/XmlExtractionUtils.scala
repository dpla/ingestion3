package dpla.ingestion3.mappers.xml

import scala.xml._

trait XmlExtractionUtils {

  /**
    *
    * @param fieldName
    * @param xml
    * @return
    */
  def extractString(fieldName: String)(implicit xml: Elem): Option[String]
  = extractString(xml \\ fieldName)

  /**
    *
    */
  def extractStrings(fieldName: String)(implicit xml: Elem): Seq[String]
  = extractStrings(xml \\ fieldName)

  /**
    *
    * @param xValue
    * @return
    */
  def extractString(xValue: NodeSeq): Option[String] = {
    xValue match {
      case v if (v.text.nonEmpty) => Some(v.text.trim)
      case _ => None
    }
  }

  /**
    * TODO swing back and deeper dive into NodeSeq vs JValue/JObject
    *
    * @param xValue
    * @return
    */
  def extractStrings(xValue: NodeSeq): Seq[String] = xValue match {
    case v if v.size > 1 => v.flatMap( value => extractString(value))
    case _ => extractString(xValue) match {
      case Some(stringValue) => Seq(stringValue)
      case _ => Seq()
    }
  }
}
