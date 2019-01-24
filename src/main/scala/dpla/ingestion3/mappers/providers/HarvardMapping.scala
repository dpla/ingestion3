package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.{Document, IdMinter, Mapping, XmlExtractor}
import dpla.ingestion3.messages.IngestMessageTemplates
import dpla.ingestion3.model.DplaMapData.{ExactlyOne, ZeroToMany}
import dpla.ingestion3.model.{EdmAgent, EdmWebResource, URI}
import org.json4s.JsonAST
import org.json4s.JsonAST.JNull

import scala.xml.NodeSeq

class HarvardMapping
  extends Mapping[NodeSeq] with XmlExtractor with IdMinter[NodeSeq] with IngestMessageTemplates {

  override def dplaUri(data: Document[NodeSeq]): ExactlyOne[URI] = ???

  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] = ???

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] = ???

  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] = ???

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = ???

  override def sidecar(data: Document[NodeSeq]): JsonAST.JValue = JNull

  /**
    * Does the provider use a prefix (typically their provider shortname/abbreviation) to
    * salt the base identifier?
    *
    * @return Boolean
    */
  override def useProviderName: Boolean = true

  /**
    * Extract the record's "persistent" identifier. Implementations should raise
    * an Exception if no ID can be extracted
    *
    * @return String Record identifier
    * @throws Exception If ID can not be extracted
    */
  override def getProviderId(implicit data: Document[NodeSeq]): String = ???
}
