package dpla.ingestion3.mappers.utils

import org.json4s.JsonAST._

import scala.util.{Try, Success, Failure}
import scala.xml.{Elem, MetaData, Node, NodeSeq, Text}

/** For ripping data out of original records
  */
trait Extractor[T] {
  // Extract zero or one value
  def extractString(fieldName: String)(implicit data: T): Option[String]
  def extractString(data: T): Option[String]

  // Extract zero to many values
  def extractStrings(fieldName: String)(implicit data: T): Seq[String]
  def extractStrings(data: T): Seq[String]
}

trait XmlExtractor extends Extractor[NodeSeq] {

  // list of common xml namespaces
  protected val dc = "http://purl.org/dc/elements/1.1/"
  protected val dcterms = "http://purl.org/dc/terms/"
  protected val edm = "http://www.europeana.eu/schemas/edm/"
  protected val foaf = "http://xmlns.com/foaf/0.1/"
  protected val mods = "http://www.loc.gov/mods/v3"
  protected val oclcdc = "http://worldcat.org/xmlschemas/oclcdc-1.0/"
  protected val oai_dc = "http://www.openarchives.org/OAI/2.0/oai_dc/"
  protected val oai_pmh = "http://www.openarchives.org/OAI/2.0/"
  protected val owl = "http://www.w3.org/2002/07/owl#"
  protected val qdc = "http://worldcat.org/xmlschemas/qdc-1.0/"
  protected val rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
  protected val rdfa = "http://www.w3.org/ns/rdfa#"
  protected val skos = "http://www.w3.org/2004/02/skos/core#"
  protected val xsf = "http://www.w3.org/2001/XMLSchema#"
  protected val xsi = "http://www.w3.org/2001/XMLSchema-instance"

  override def extractString(fieldName: String)(implicit
      xml: NodeSeq
  ): Option[String] =
    extractString(xml \ fieldName)

  def extractStrings(fieldName: String)(implicit xml: NodeSeq): Seq[String] =
    extractStrings(xml \ fieldName)

  override def extractString(xValue: NodeSeq): Option[String] =
    xValue match {
      case v if v.text.nonEmpty => Some(v.text)
      case _                    => None
    }

  def extractString(xValue: Node): Option[String] =
    xValue match {
      case v if v.text.nonEmpty => Some(v.text)
      case _                    => None
    }

  override def extractStrings(xValue: NodeSeq): Seq[String] =
    (xValue match {
      case v if v.size > 1 => v.flatMap(value => extractString(value))
      case _ =>
        extractString(xValue) match {
          case Some(stringValue) => Seq(stringValue)
          case _                 => Seq()
        }
    }).filter(_.nonEmpty)


  /** Exclude nodes that do not have an attribute that matches att and value
    * parameters
    *
    * @param node
    *   Node XML node to examine
    * @param att
    *   String name of attribute
    * @param value
    *   String value of attribute
    * @return
    *   Boolean
    */
  def filterAttribute(node: Node, att: String, value: String): Boolean =
    (node \ ("@" + att)).text.toLowerCase == value.toLowerCase

  /** Get Nodes that match the given attribute name and attribute value
    * @param e
    *   Elem to examine
    * @param att
    *   String name of attribute
    * @param value
    *   String value of attribute
    * @return
    *   NodeSeq of matching nodes
    */
  def getByAttribute(e: Elem, att: String, value: String): NodeSeq =
    e \\ "_" filter { n => filterAttribute(n, att, value) }

  def getByAttribute(e: NodeSeq, att: String, value: String): NodeSeq =
    getByAttribute(e.asInstanceOf[Elem], att, value)

  /** Exclude nodes that do not have an attribute that matches att and any of
    * the given value parameters
    *
    * @param node
    *   Node XML node to examine
    * @param att
    *   String name of attribute
    * @param values
    *   Seq[String] all values of attributes
    * @return
    *   Boolean
    */
  def filterAttributeListOptions(
      node: Node,
      att: String,
      values: Seq[String]
  ): Boolean =
    values.map(_.toLowerCase).contains((node \ ("@" + att)).text.toLowerCase)

  /** Get nodes that match any of the given values for a given attribute.
    *
    * E.g. getByAttributesListOptions(elem, "color", Seq("red", "blue")) will
    * return all nodes where attribute "color=red" or "color=blue"
    *
    * @param e
    *   Elem to examine
    * @param att
    *   String name of attribute
    * @param values
    *   Seq[String] Values of attribute
    * @return
    *   NodeSeq of matching nodes
    */
  def getByAttributeListOptions(
      e: Elem,
      att: String,
      values: Seq[String]
  ): NodeSeq =
    e \\ "_" filter { n => filterAttributeListOptions(n, att, values) }

  def getByAttributeListOptions(
      e: NodeSeq,
      att: String,
      values: Seq[String]
  ): NodeSeq =
    getByAttributeListOptions(e.asInstanceOf[Elem], att, values)

  /** For each given node, get any immediate children that are text values.
    * Ignore nested text values.
    */
  def extractChildStrings(xValue: NodeSeq): Seq[String] = xValue.flatMap {
    node =>
      node.child.collect { case Text(t) => t }.map(_.trim).filterNot(_.isEmpty)
  }

  def getAttributeValue(node: Node, attribute: String): Option[String] =
    node.attributes.headOption match {
      case Some(metadata: MetaData) =>
        Try {
          metadata.asAttrMap(attribute)
        } match {
          case Success(s) => Some(s)
          case Failure(_) => None
        }
      case _ => None
    }
}

trait JsonExtractor extends Extractor[JValue] {

  /** Pulls a single string from the implicit json data.
    *
    * @param fieldName
    *   Name of field to extract string from.
    * @param json
    *   JSON document or sub document
    * @return
    *   Some(string) if one could be found. Will fail on non-primitive values
    *   like arrays or objects.
    */
  def extractString(fieldName: String)(implicit json: JValue): Option[String] =
    extractString(json \ fieldName)

  /** Pulls a Seq[String] of values from the implicit json
    *
    * @param fieldName
    *   Name of field to extract string from.
    * @param json
    *   JSON document or sub document
    * @return
    *   A Seq[String].
    *
    * If the field is an array, the Seq will contain the array contents, if they
    * were themselves primitive values.
    *
    * If the field is an object, the Seq will contain the object field values,
    * if those were primitive values.
    *
    * If the field is a primitive, a Seq with a single member is returned.
    *
    * Otherwise, an empty Seq is returned.
    */
  def extractStrings(fieldName: String)(implicit json: JValue): Seq[String] =
    extractStrings(json \ fieldName)


  /** @see
    *   definition of extractStrings(fieldName: String), save for this version
    *   can be called with a parameter that generates a JValue, such as json4s'
    *   path-walking syntax: jsonTree \ "someChild" \\ "someDecendant"
    */
  def extractStrings(jValue: JValue): Seq[String] = {
    val values = jValue match {
      case JArray(array) => array.flatMap(entry => extractString(entry))
      case JObject(fields) =>
        fields.flatMap({ case (_, value) => extractString(value) })
      case _ =>
        extractString(jValue) match {
          case Some(stringValue) => Seq(stringValue)
          case None              => Seq()
        }
    }
    values.filter(_.nonEmpty)
  }

  /** @see
    *   definition of extractString(fieldName: String), save for this version
    *   can be called with a parameter that generates a JValue, such as json4s'
    *   path-walking syntax: jsonTree \ "someChild" \\ "someDecendant"
    */
  def extractString(jValue: JValue): Option[String] = jValue match {
    case JBool(bool)       => Some(bool.toString)
    case JDecimal(decimal) => Some(decimal.toString)
    case JDouble(double)   => Some(double.toString)
    case JInt(int)         => Some(int.toString())
    case JString(string)   => Some(string)
    case _                 => None
  }

  /** Sometimes we need to map the keys
    */
  def extractKeys(jValue: JValue): Seq[String] = jValue match {
    case JArray(array)   => array.flatMap(entry => extractKeys(entry))
    case JObject(fields) => fields.map { case (field, _) => field }
    case _               => Seq()
  }

  /** Wraps the JValue in JArray if it is not already a JArray
    * Addresses the problem of inconsistent data types in value
    * field. e.g. '"prop": ["val1"]'
    * vs '"prop": "val1"'
    */
  // TODO Can we make this an implicit?
  def iterify(jValue: JValue): JArray = jValue match {
    case JArray(j) => JArray(j)
    case JNothing  => JArray(List())
    case _         => JArray(List(jValue))
  }
}