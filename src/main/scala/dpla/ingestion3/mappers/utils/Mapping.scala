package dpla.ingestion3.mappers.utils

import java.net.URI

import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model._
import org.json4s.JValue

import scala.xml.NodeSeq

//noinspection TypeAnnotation
trait Mapping[T] {
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


// TODO Follow-up with micheal. This repetition seems unnecessary but I couldn't get it to compile without it.

trait XmlMapping extends Mapping[NodeSeq] {
  // OreAggregation
  override def dplaUri(data: NodeSeq): ExactlyOne[URI]
  override def dataProvider(data: NodeSeq): ExactlyOne[EdmAgent]
  override def originalRecord(data: NodeSeq): ExactlyOne[String]
  override def hasView(data: NodeSeq): ZeroToMany[EdmWebResource] = Seq()
  override def intermediateProvider(data: NodeSeq): ZeroToOne[EdmAgent] = None
  override def isShownAt(data: NodeSeq): ExactlyOne[EdmWebResource]
  override def `object`(data: NodeSeq): ZeroToOne[EdmWebResource] = None // full size image
  override def preview(data: NodeSeq): ZeroToOne[EdmWebResource] = None // thumbnail
  override def provider(data: NodeSeq): ExactlyOne[EdmAgent]
  override def edmRights(data: NodeSeq): ZeroToOne[URI] = None
  override def sidecar(data: NodeSeq): JValue

  // SourceResource
  override def alternateTitle(data: NodeSeq): ZeroToMany[String] = Seq()
  override def collection(data: NodeSeq): ZeroToMany[DcmiTypeCollection] = Seq()
  override def contributor(data: NodeSeq): ZeroToMany[EdmAgent] = Seq()
  override def creator(data: NodeSeq): ZeroToMany[EdmAgent] = Seq()
  override def date(data: NodeSeq): ZeroToMany[EdmTimeSpan] = Seq()
  override def description(data: NodeSeq): ZeroToMany[String] = Seq()
  override def extent(data: NodeSeq): ZeroToMany[String] = Seq()
  override def format(data: NodeSeq): ZeroToMany[String] = Seq()
  override def genre(data: NodeSeq): ZeroToMany[SkosConcept] = Seq()
  override def identifier(data: NodeSeq): ZeroToMany[String] = Seq()
  override def language(data: NodeSeq): ZeroToMany[SkosConcept] = Seq()
  override def place(data: NodeSeq): ZeroToMany[DplaPlace] = Seq()
  override def publisher(data: NodeSeq): ZeroToMany[EdmAgent] = Seq()
  override def relation(data: NodeSeq): ZeroToMany[LiteralOrUri] = Seq()
  override def replacedBy(data: NodeSeq): ZeroToMany[String] = Seq()
  override def replaces(data: NodeSeq): ZeroToMany[String] = Seq()
  override def rights(data: NodeSeq): AtLeastOne[String] = Seq()
  override def rightsHolder(data: NodeSeq): ZeroToMany[EdmAgent] = Seq()
  override def subject(data: NodeSeq): ZeroToMany[SkosConcept] = Seq()
  override def temporal(data: NodeSeq): ZeroToMany[EdmTimeSpan] = Seq()
  override def title(data: NodeSeq): AtLeastOne[String] = Seq()
  override def `type`(data: NodeSeq): ZeroToMany[String] = Seq()
}

trait JsonMapping extends Mapping[JValue]