package dpla.ingestion3.mappers

import java.net.URI

import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model._
import dpla.ingestion3.utils.ProviderRegistry
import org.json4s.JValue
import org.json4s.JsonAST._

import scala.util.Try
import scala.xml.{NodeSeq, XML}




object MyMapper {

  def build(shortname: String, data: String): Try[OreAggregation] = {

    val reg = ProviderRegistry.lookupRegister(shortname)
      .getOrElse(throw new RuntimeException(s"Failed to load registry for '$shortname'"))

    val originalRecord = reg.parser.parse(data)
    val mapper = reg.mapper

    Try {
      OreAggregation(
        dplaUri = mapper.dplaUri(originalRecord),
        dataProvider = mapper.dataProvider(originalRecord),
        originalRecord = data,
        hasView = mapper.hasView(originalRecord),
        intermediateProvider = mapper.intermediateProvider(originalRecord),
        isShownAt = mapper.isShownAt(originalRecord),
        `object` = mapper.`object`(originalRecord), // full size image
        preview = mapper.preview(originalRecord), // thumbnail
        provider = mapper.provider(originalRecord),
        edmRights = mapper.edmRights(originalRecord),
        sidecar = mapper.sidecar(originalRecord),
        sourceResource = DplaSourceResource()
      )
    }
  }
}




trait MappingTools[T] {
  type Parser
  type Extractor
  type ParsedRepresentation
}

trait XmlMappingTools extends MappingTools[NodeSeq] {
  override type Parser = Parser
  override type Extractor = XmlExtractor
  override type ParsedRepresentation = NodeSeq
}

trait JsonMappingTools extends MappingTools[JValue] {
  override type Parser = Parser
  override type Extractor = JsonExtractor
  override type ParsedRepresentation = JValue
}


//noinspection TypeAnnotation
trait Mapper[T] {

  // OreAggregation
  def dplaUri(data: T): ExactlyOne[URI]
  def dataProvider(data: T): ExactlyOne[EdmAgent]
  def originalRecord(data: T): ExactlyOne[String]
  def hasView(data: T): ZeroToMany[EdmWebResource] = Seq()
  def intermediateProvider(data: T): ZeroToOne[EdmAgent] = None
  def isShownAt(data: T): ExactlyOne[EdmWebResource]
  def `object`(data: T): ZeroToOne[EdmWebResource] = None // full size image
  def preview(data: T): ZeroToOne[EdmWebResource] = None // thumbnail
  def provider(data: T): ExactlyOne[EdmAgent]
  def edmRights(data: T): ZeroToOne[URI] = None
  def sidecar(data: T): JValue

  // SourceResource
  def alternateTitle(data: T): ZeroToMany[String] = Seq()
  def collection(data: T): ZeroToMany[DcmiTypeCollection] = Seq()
  def contributor(data: T): ZeroToMany[EdmAgent] = Seq()
  def creator(data: T): ZeroToMany[EdmAgent] = Seq()
  def date(data: T): ZeroToMany[EdmTimeSpan] = Seq()
  def description(data: T): ZeroToMany[String] = Seq()
  def extent(data: T): ZeroToMany[String] = Seq()
  def format(data: T): ZeroToMany[String] = Seq()
  def genre(data: T): ZeroToMany[SkosConcept] = Seq()
  def identifier(data: T): ZeroToMany[String] = Seq()
  def language(data: T): ZeroToMany[SkosConcept] = Seq()
  def place(data: T): ZeroToMany[DplaPlace] = Seq()
  def publisher(data: T): ZeroToMany[EdmAgent] = Seq()
  def relation(data: T): ZeroToMany[LiteralOrUri] = Seq()
  def replacedBy(data: T): ZeroToMany[String] = Seq()
  def replaces(data: T): ZeroToMany[String] = Seq()
  def rights(data: T): AtLeastOne[String] = Seq()
  def rightsHolder(data: T): ZeroToMany[EdmAgent] = Seq()
  def subject(data: T): ZeroToMany[SkosConcept] = Seq()
  def temporal(data: T): ZeroToMany[EdmTimeSpan] = Seq()
  def title(data: T): AtLeastOne[String] = Seq()
  def `type`(data: T): ZeroToMany[String] = Seq()
}

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
class XmlExtractor extends Extractor[NodeSeq] {

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
class JsonExtractor extends Extractor[JValue] {


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
