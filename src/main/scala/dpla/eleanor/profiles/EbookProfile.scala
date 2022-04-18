package dpla.eleanor.profiles

import dpla.eleanor.Schemata.Payload
import dpla.eleanor.{PayloadMapper, XmlPayloadMapper}
import dpla.ingestion3.harvesters.Harvester
import dpla.ingestion3.mappers.utils._
import dpla.ingestion3.mappers.{JsonMapper, Mapper, XmlMapper}
import dpla.ingestion3.model.DplaMapData.ZeroToMany
import dpla.ingestion3.model.OreAggregation
import org.json4s.JValue

import scala.xml.NodeSeq

// Generic profile for Ebook and CH
trait Profile[T] extends Serializable {
  def mapOreAggregation(data: String): OreAggregation
}

trait EbookProfile[T] extends Profile[T] {

  // TODO This should not be an instance of a Class
  def getHarvester: Class[_ <: Harvester]
  def getParser: Parser[T]
  def getMapper: Mapper[T, Mapping[T]]
  def getMapping: Mapping[T]

  def getPayloadMapper: PayloadMapper[T, Mapping[T]]

  override def mapOreAggregation(data: String): OreAggregation = {
    val parser = getParser // XML or JSON parser
    val mapping = getMapping // XML or JSON mapping
    val mapper = getMapper // OreAggregation mapper

    val document = parser.parse(data)
    val oreAggregation = mapper.map(document, mapping)

    oreAggregation
  }

  // Not overriden
  def mapPayload(data: String): ZeroToMany[Payload] = {
    val parser = getParser // XML or JSON parser
    val mapping = getMapping // XML or JSON mapping
    val payloadMapper = getPayloadMapper   // Payload mapper

    val document = parser.parse(data)
    val payloads = payloadMapper.map(document, mapping)

    payloads
  }
}

trait JsonProfile extends EbookProfile[JValue] {
  override def getMapper = new JsonMapper
  override def getParser = new JsonParser

  // No Json ebook providers yet
}

trait XmlProfile extends EbookProfile[NodeSeq] {
  override def getParser = new XmlParser
  override def getMapper = new XmlMapper

  override def getPayloadMapper = new XmlPayloadMapper
}


