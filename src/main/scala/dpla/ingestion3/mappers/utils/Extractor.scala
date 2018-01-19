package dpla.ingestion3.mappers.utils

import org.json4s.JValue
import org.json4s.JsonAST._

import scala.xml.NodeSeq


/**
  * For ripping data out of original records
  *
  * @tparam T
  */
trait Extractor[T] {
  // Extract zero or one value
  def extractString(fieldname: String)(implicit data: T): Option[String]
  def extractString(data: T): Option[String]

  // Extract zero to many values
  def extractStrings(fieldname: String)(implicit data: T): Seq[String]
  def extractStrings(data: T): Seq[String]
}

/**
  * XML Extractor
  */
trait XmlExtractor extends Extractor[NodeSeq] {

  /**
    *
    * @param fieldName
    * @param xml
    * @return
    */
  override def extractString(fieldName: String)(implicit xml: NodeSeq): Option[String]
  = extractString(xml \ fieldName)

  /**
    *
    */
  def extractStrings(fieldName: String)(implicit xml: NodeSeq): Seq[String]
  = extractStrings(xml \ fieldName)

  /**
    *
    * @param xValue
    * @return
    */
  override def extractString(xValue: NodeSeq): Option[String] = {
    xValue match {
      case v if v.text.nonEmpty => Some(v.text)
      case _ => None
    }
  }

  /**
    * TODO swing back and deeper dive into NodeSeq vs JValue/JObject
    *
    * @param xValue
    * @return Seq[String]
    */
  override def extractStrings(xValue: NodeSeq): Seq[String] = xValue match {
    case v if v.size > 1 => v.flatMap(value => extractString(value))
    case _ => extractString(xValue) match {
      case Some(stringValue) => Seq(stringValue)
      case _ => Seq()
    }
  }
}

/**
  * Json extractor
  */
trait JsonExtractor extends Extractor[JValue] {


  /**
    * Pulls a single string from the implicit json data.
    *
    * @param fieldName Name of field to extract string from.
    * @param json      JSON document or subdocument
    * @return Some(string) if one could be found. Will fail
    *         on non-primitive values like arrays or objects.
    */
  def extractString(fieldName: String)(implicit json: JValue): Option[String]
  = extractString(json \ fieldName)

  /**
    * Pulls a Seq[String] of values from the implicit json daa
    *
    * @param fieldName Name of field to extract string from.
    * @param json      JSON document or subdocument
    * @return A Seq[String].
    *
    *         If the field is an array, the Seq will contain the array contents,
    *         if they were themselves primitive values.
    *
    *         If the field is an object, the Seq will contain the object field
    *         values, if those were primitive values.
    *
    *         If the field is a primitive, a Seq with a single member is returned.
    *
    *         Otherwise, an empty Seq is returned.
    */
  def extractStrings(fieldName: String)(implicit json: JValue): Seq[String]
  = extractStrings(json \ fieldName)

  /**
    * @see definition of extractStrings(fieldName: String), save for this version
    *      can be called with a parameter that generates a JValue, such as json4s'
    *      path-walking syntax: jsonTree \ "someChild" \\ "someDecendant"
    *
    */
  def extractStrings(jValue: JValue): Seq[String] = jValue match {
    case JArray(array) => array.flatMap(entry => extractString(entry))
    case JObject(fields) => fields.flatMap({case (_, value) => extractString(value)})
    case _ => extractString(jValue) match {
      case Some(stringValue) => Seq(stringValue)
      case None => Seq()
    }
  }

  /**
    * @see definition of extractString(fieldName: String), save for this version
    *      can be called with a parameter that generates a JValue, such as json4s'
    *      path-walking syntax: jsonTree \ "someChild" \\ "someDecendant"
    *
    */
  def extractString(jValue: JValue): Option[String] = jValue match {
    case JBool(bool) => Some(bool.toString)
    case JDecimal(decimal) => Some(decimal.toString)
    case JDouble(double) => Some(double.toString)
    case JInt(int) => Some(int.toString())
    case JString(string) => Some(string)
    case _ => None
  }

  /**
    * Wraps the JValue in JArray if it is not already a JArray
    * Addresses the problem of inconsistent data types in value field.
    * e.g. '"prop": ["val1"]' vs '"prop": "val1"'
    *
    * @param jvalue
    * @return
    */
  def iterify(jvalue: JValue): JArray = jvalue match {
    case JArray(j) => JArray(j)
    case JNothing => JArray(List())
    case _ => JArray(List(jvalue))
  }
}
