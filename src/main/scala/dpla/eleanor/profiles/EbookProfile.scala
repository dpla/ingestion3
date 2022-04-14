package dpla.eleanor.profiles

import dpla.eleanor.Schemata.Payload
import dpla.ingestion3.harvesters.Harvester
import dpla.ingestion3.mappers.utils._
import dpla.ingestion3.mappers.{JsonMapper, Mapper, XmlMapper}
import dpla.ingestion3.messages.{IngestMessage, IngestMessageTemplates, MessageCollector}
import dpla.ingestion3.model.DplaMapData.ZeroToMany
import dpla.ingestion3.model.OreAggregation
import org.json4s.JValue

import scala.xml.NodeSeq

// Generic profile for Ebook and CH
trait Profile[T] {
  def performMapping(data: String): OreAggregation
}

trait EbookProfile[T] extends Profile[T] {

  // TODO This should not be an instance of a Class
  def getHarvester: Class[_ <: Harvester]
  def getParser: Parser[T]
  def getMapper: Mapper[T, Mapping[T]]
  def getMapping: Mapping[T]

  def getPayloadMapper: PayloadMapper[T, Mapping[T]]

  override def performMapping(data: String): OreAggregation = {
    val parser = getParser
    val mapping = getMapping
    val mapper = getMapper

    val document = parser.parse(data)
    val oreAggregation = mapper.map(document, mapping)

    oreAggregation
  }

  def performPayloadMapping(data: String): ZeroToMany[Payload] = {
    val parser = getParser
    val mapping = getMapping
    val payloadMapper = getPayloadMapper

    val document = parser.parse(data)
    val payloads = payloadMapper.map(document, mapping)

    payloads
  }
}

trait JsonProfile extends EbookProfile[JValue] {
  override def getMapper = new JsonMapper
  override def getParser = new JsonParser
}

trait XmlProfile extends EbookProfile[NodeSeq] {
  override def getParser = new XmlParser
  override def getMapper = new XmlMapper

  override def getPayloadMapper = new XmlPayloadMapper
}

trait PayloadMapper[T, +E] extends IngestMessageTemplates {
  def map(document: Document[T], mapping: Mapping[T]): ZeroToMany[Payload]
}

class XmlPayloadMapper extends PayloadMapper[NodeSeq, XmlMapping] {
  override def map(document: Document[NodeSeq], mapping: Mapping[NodeSeq]): ZeroToMany[Payload] = {

    implicit val msgCollector: MessageCollector[IngestMessage] = new MessageCollector[IngestMessage]

    mapping.payloads(document)
  }
}
