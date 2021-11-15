package dpla.ingestion3.profiles

import dpla.ingestion3.harvesters.Harvester
import dpla.ingestion3.mappers.providers.NyplMapping
import dpla.ingestion3.mappers.utils._
import dpla.ingestion3.mappers.{JsonMapper, Mapper, XmlMapper}
import dpla.ingestion3.model.OreAggregation
import org.json4s.JValue
import org.json4s.jackson.JsonMethods.parse

import scala.xml.NodeSeq

trait IngestionProfile[T] {

  // TODO This should not be an instance of a Class
  def getHarvester: Class[_ <: Harvester]
  def getParser: Parser[T]
  def getMapper: Mapper[T, Mapping[T]]
  def getMapping: Mapping[T]


  def performMapping(data: String): OreAggregation = {
    val parser = getParser
    val mapping = getMapping
    val mapper = getMapper

    val document = parser.parse(data)
    mapper.map(document, mapping)
  }
}

trait JsonProfile extends IngestionProfile[JValue] {
  override def getMapper = new JsonMapper
  override def getParser = new JsonParser
}

trait XmlProfile extends IngestionProfile[NodeSeq] {
  override def getParser = new XmlParser
  override def getMapper = new XmlMapper
}

/**
  * This is weird
  */
trait NyplIngestionProfile extends IngestionProfile[JValue] {
  override def getMapper = new JsonMapper
  override def getParser = new JsonParser

  override def performMapping(data: String): OreAggregation = {
    val json = Document(parse(data))
    val parser = getParser
    val mapping = new NyplMapping(json)
    val mapper = getMapper

    val document = parser.parse(data)
    mapper.map(document, mapping)
  }
}

