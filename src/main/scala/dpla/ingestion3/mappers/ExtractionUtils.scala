package dpla.ingestion3.mappers

import java.net.URI

import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model._
import org.json4s.JsonAST.{JNothing, JValue}

import scala.xml.{NodeSeq, XML}

//noinspection TypeAnnotation
trait ExtractionMapper[T] {
  // OreAggregation
  def dplaUri(data: T): ExactlyOne[URI]
  def dataProvider(data: T): ExactlyOne[EdmAgent]
  def originalRecord(data: T): ExactlyOne[String]
  def hasView(data: T): ZeroToMany[EdmWebResource]
  def intermediateProvider(data: T): ZeroToOne[EdmAgent]
  def isShownAt(data: T): ExactlyOne[EdmWebResource]
  def `object`(data: T): ZeroToOne[EdmWebResource] // full size image
  def preview(data: T): ZeroToOne[EdmWebResource] // thumbnail
  def provider(data: T): ExactlyOne[EdmAgent]
  def edmRights(data: T): ZeroToOne[URI]
  def sidecar(data: T): JValue

  // SourceResource
  def alternateTitle(data: T): ZeroToMany[String]
  def collection(data: T): ZeroToMany[DcmiTypeCollection]
  def contributor(data: T): ZeroToMany[EdmAgent]
  def creator(data: T): ZeroToMany[EdmAgent]
  def date(data: T): ZeroToMany[EdmTimeSpan]
  def description(data: T): ZeroToMany[String]
  def extent(data: T): ZeroToMany[String]
  def format(data: T): ZeroToMany[String]
  def genre(data: T): ZeroToMany[SkosConcept]
  def identifier(data: T): ZeroToMany[String]
  def language(data: T): ZeroToMany[SkosConcept]
  def place(data: T): ZeroToMany[DplaPlace]
  def publisher(data: T): ZeroToMany[EdmAgent]
  def relation(data: T): ZeroToMany[LiteralOrUri]
  def replacedBy(data: T): ZeroToMany[String]
  def replaces(data: T): ZeroToMany[String]
  def rights(data: T): AtLeastOne[String]
  def rightsHolder(data: T): ZeroToMany[EdmAgent]
  def subject(data: T): ZeroToMany[SkosConcept]
  def temporal(data: T): ZeroToMany[EdmTimeSpan]
  def title(data: T): AtLeastOne[String]
  def `type`(data: T): ZeroToMany[String]
}


/**
  * Parses original records
  *
  * @tparam T
  */
trait ExtractionParser[T] {
  def parse(data: String): T
}

/**
  * For ripping data out of original records
  *
  * @tparam T
  */
trait Extractor[T] {

  // Extract data
  def extractString(fieldname: String)(implicit data: T): Option[String]
  def extractString(data: T): Option[String]

  def extractStrings(fieldname: String)(implicit data: T): Seq[String]
  def extractStrings(data: T): Seq[String]
}


/**
  * Generalized Extractor
  */
object ExtractionUtils {

  /**
    * XML Extractor
    */
  implicit object XmlExtractionUtils extends Extractor[NodeSeq] with ExtractionParser[NodeSeq] {

    /**
      *
      * @param data
      * @return
      */
    override def parse(data: String): NodeSeq = XML.loadString(data)

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
  implicit object JsonExtractionUtils extends Extractor[JValue]
    with ExtractionParser[JValue] {
    override def parse(data: String): JValue = org.json4s.jackson.JsonMethods.parse(data)

    override def extractStrings(fieldname: String)(implicit data: JValue): Seq[String] = ???

    override def extractStrings(data: JValue): Seq[String] = ???

    override def extractString(fieldname: String)(implicit data: JValue): Option[String] = ???

    override def extractString(data: JValue): Option[String] = ???
  }
}
