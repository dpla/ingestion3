package dpla.ingestion3.mappers.utils

import java.net.URI

import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model._
import org.json4s.JValue

import scala.xml.NodeSeq


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