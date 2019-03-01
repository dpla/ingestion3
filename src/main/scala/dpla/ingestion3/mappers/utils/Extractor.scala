package dpla.ingestion3.mappers.utils

import org.json4s.JsonAST._

import scala.xml.{Elem, Node, NodeSeq, Text}


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
  override def extractString(fieldName: String)(implicit xml: NodeSeq): Option[String] =
    extractString(xml \ fieldName)

  /**
    *
    */
  def extractStrings(fieldName: String)(implicit xml: NodeSeq): Seq[String] =
    extractStrings(xml \ fieldName)

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

  /**
    * Exclude nodes that do not have an attribute that matches att and value parameters
    *
    * @param node Node XML node to examine
    * @param att String name of attribute
    * @param value String value of attribute
    * @return Boolean
    */
  def filterAttribute(node: Node, att: String, value: String): Boolean = (node \ ("@" + att)).text.toLowerCase == value

  /**
    * Get Nodes that match the given attribute name and attribute value
    * @param e Elem to examine
    * @param att String name of attribute
    * @param value String value of attribute
    * @return NodeSeq of matching nodes 
    */
  def getByAttribute(e: Elem, att: String, value: String): NodeSeq = {
    e \\ "_" filter { n => filterAttribute(n, att, value) }
  }

  def getByAttribute(e: NodeSeq, att: String, value: String): NodeSeq = {
    getByAttribute(e.asInstanceOf[Elem], att, value)
  }

  /**
    * For each given node, get any immediate children that are text values.
    * Ignore nested text values.
    *
    * E.g. <foo>bar</foo> => "bar"
    * E.g. <foo><bar>bat</bar></foo> => nothing
    *
    * @param xValue NodeSeq
    * @return Seq[String]
    */
  def extractChildStrings(xValue: NodeSeq): Seq[String] = xValue.flatMap { node =>
    node.child.collect{ case Text(t) => t }.map(_.trim).filterNot(_.isEmpty)
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
  def extractString(fieldName: String)(implicit json: JValue): Option[String] =
    extractString(json \ fieldName)

  /**
    * Pulls a Seq[String] of values from the implicit json
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
    * Pulls a Seq[String] of values from the implicit json from the specified field name
    * at any depth
    *
    * @param fieldName
    * @param json
    * @return
    */
  def extractStringsDeep(fieldName: String)(implicit json: JValue): Seq[String]
    = extractStrings(json \\ fieldName)

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
    * Sometimes we need to map the keys
    *
    * @param jValue
    * @return
    */
  def extractKeys(jValue: JValue): Seq[String] = jValue match {
    case JArray(array) => array.flatMap(entry => extractKeys(entry))
    case JObject(fields) => fields.map { case(field, _) => field }
    case _ => Seq()
  }

  /**
    * Wraps the JValue in JArray if it is not already a JArray
    * Addresses the problem of inconsistent data types in value field.
    * e.g. '"prop": ["val1"]' vs '"prop": "val1"'
    *
    * @param jvalue
    * @return
    */
  // TODO Can we make this an implicit?
  def iterify(jvalue: JValue): JArray = jvalue match {
    case JArray(j) => JArray(j)
    case JNothing => JArray(List())
    case _ => JArray(List(jvalue))
  }
}
