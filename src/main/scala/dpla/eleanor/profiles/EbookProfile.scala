package dpla.eleanor.profiles

import dpla.ingestion3.harvesters.Harvester
import dpla.ingestion3.mappers.providers.NyplMapping
import dpla.ingestion3.mappers.utils._
import dpla.ingestion3.mappers.{JsonMapper, Mapper, XmlMapper}
import dpla.ingestion3.model.OreAggregation
import org.json4s.JValue
import org.json4s.jackson.JsonMethods.parse

import scala.xml.NodeSeq

trait Profile[T] {
  def performMapping(data: String): OreAggregation
}

trait EbookProfile[T] extends Profile[T] {

  // TODO This should not be an instance of a Class
  def getHarvester: Class[_ <: Harvester]
  def getParser: Parser[T]
  def getMapper: Mapper[T, Mapping[T]]
  def getMapping: Mapping[T]


  override def performMapping(data: String): OreAggregation = {
    val parser = getParser
    val mapping = getMapping
    val mapper = getMapper

    val document = parser.parse(data)
    mapper.map(document, mapping)
  }
}

trait JsonProfile extends EbookProfile[JValue] {
  override def getMapper = new JsonMapper
  override def getParser = new JsonParser
}

trait XmlProfile extends EbookProfile[NodeSeq] {
  override def getParser = new XmlParser
  override def getMapper = new XmlMapper
}

