package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.{Document, XmlExtractor, XmlMapping}
import dpla.ingestion3.messages.IngestMessageTemplates
import dpla.ingestion3.model.DplaMapData.{ExactlyOne, ZeroToMany, ZeroToOne}
import dpla.ingestion3.model._
import org.json4s

import scala.xml._

class VtMapping extends XmlMapping with XmlExtractor with IngestMessageTemplates {

  override def dplaUri(data: Document[NodeSeq]): ZeroToOne[URI] = ???

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] = ???

  override def useProviderName: Boolean = ???

  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] = ???

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = ???

  override def sidecar(data: Document[NodeSeq]): json4s.JValue = ???

  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] = ???

  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] = ???

}
