package dpla.eleanor.mappers.utils

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
  def filterAttribute(node: Node, att: String, value: String): Boolean = (node \ ("@" + att)).text.toLowerCase == value.toLowerCase

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
    * Exclude nodes that do not have an attribute that matches att and any of the given value parameters
    *
    * @param node Node XML node to examine
    * @param att String name of attribute
    * @param values Seq[String] all values of attributes
    * @return Boolean
    */
  def filterAttributeListOptions(node: Node, att: String, values: Seq[String]): Boolean =
    values.map(_.toLowerCase).contains((node \ ("@" + att)).text.toLowerCase)

  /**
    * Get nodes that match any of the given values for a given attribute.
    *
    * E.g. getByAttributesListOptions(elem, "color", Seq("red", "blue")) will return all nodes where
    *   attribute "color=red" or "color=blue"
    *
    * @param e Elem to examine
    * @param att String name of attribute
    * @param values Seq[String] Values of attribute
    * @return NodeSeq of matching nodes
    */
  def getByAttributeListOptions(e: Elem, att: String, values: Seq[String]): NodeSeq = {
    e \\ "_" filter { n => filterAttributeListOptions(n, att, values) }
  }

  def getByAttributeListOptions(e: NodeSeq, att: String, values: Seq[String]): NodeSeq = {
    getByAttributeListOptions(e.asInstanceOf[Elem], att, values)
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
