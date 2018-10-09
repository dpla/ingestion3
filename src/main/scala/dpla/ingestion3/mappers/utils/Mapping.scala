package dpla.ingestion3.mappers.utils

import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model._
import org.json4s.JsonAST.JValue

import scala.xml.NodeSeq

trait Mapping[T] {

  implicit def unwrap(document: Document[T]): T = document.get

  // OreAggregation
  def dplaUri(data: Document[T]): ExactlyOne[URI]
  def dataProvider(data: Document[T]): ZeroToMany[EdmAgent]
  def originalRecord(data: Document[T]): ExactlyOne[String]
  def hasView(data: Document[T]): ZeroToMany[EdmWebResource] = Seq()
  def intermediateProvider(data: Document[T]): ZeroToOne[EdmAgent] = None

  def isShownAt(data: Document[T]): ZeroToMany[EdmWebResource]
  def `object`(data: Document[T]): ZeroToMany[EdmWebResource] = Seq() // full size image
  def preview(data: Document[T]): ZeroToMany[EdmWebResource] = Seq() // thumbnail

  def provider(data: Document[T]): ExactlyOne[EdmAgent]
  def edmRights(data: Document[T]): ZeroToMany[URI] = Seq()
  def sidecar(data: Document[T]): JValue

  // SourceResource
  def alternateTitle(data: Document[T]): ZeroToMany[String] = Seq()
  def collection(data: Document[T]): ZeroToMany[DcmiTypeCollection] = Seq()
  def contributor(data: Document[T]): ZeroToMany[EdmAgent] = Seq()
  def creator(data: Document[T]): ZeroToMany[EdmAgent] = Seq()
  def date(data: Document[T]): ZeroToMany[EdmTimeSpan] = Seq()
  def description(data: Document[T]): ZeroToMany[String] = Seq()
  def extent(data: Document[T]): ZeroToMany[String] = Seq()
  def format(data: Document[T]): ZeroToMany[String] = Seq()
  def genre(data: Document[T]): ZeroToMany[SkosConcept] = Seq()
  def identifier(data: Document[T]): ZeroToMany[String] = Seq()
  def language(data: Document[T]): ZeroToMany[SkosConcept] = Seq()
  def place(data: Document[T]): ZeroToMany[DplaPlace] = Seq()
  def publisher(data: Document[T]): ZeroToMany[EdmAgent] = Seq()
  def relation(data: Document[T]): ZeroToMany[LiteralOrUri] = Seq()
  def replacedBy(data: Document[T]): ZeroToMany[String] = Seq()
  def replaces(data: Document[T]): ZeroToMany[String] = Seq()
  def rights(data: Document[T]): AtLeastOne[String] = Seq()
  def rightsHolder(data: Document[T]): ZeroToMany[EdmAgent] = Seq()
  def subject(data: Document[T]): ZeroToMany[SkosConcept] = Seq()
  def temporal(data: Document[T]): ZeroToMany[EdmTimeSpan] = Seq()
  def title(data: Document[T]): AtLeastOne[String] = Seq()
  def `type`(data: Document[T]): ZeroToMany[String] = Seq()
}

trait XmlMapping extends Mapping[NodeSeq]

trait JsonMapping extends Mapping[JValue]