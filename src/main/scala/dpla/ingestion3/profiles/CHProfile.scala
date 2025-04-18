package dpla.ingestion3.profiles

import dpla.ingestion3.harvesters.Harvester
import dpla.ingestion3.mappers.utils._
import dpla.ingestion3.mappers.{JsonMapper, Mapper, XmlMapper}
import dpla.ingestion3.model.OreAggregation
import org.json4s.JValue

import scala.xml.NodeSeq

trait CHProfile[T] extends Serializable {

  // TODO This should not be an instance of a Class

  def getHarvester: Class[_ <: Harvester]

  def getParser: Parser[T]

  def getMapper: Mapper[T, Mapping[T]]

  def getMapping: Mapping[T]

  def mapOreAggregation(data: String): OreAggregation = {
    val parser = getParser
    val mapping = getMapping
    val mapper = getMapper

    val document = parser.parse(data)
    val internalDoc = mapping.preMap(document)
    val oreAggregation = mapper.map(internalDoc, mapping)
    mapping.postMap(internalDoc)
    oreAggregation
  }
}

trait JsonProfile extends CHProfile[JValue] {
  override def getMapper = new JsonMapper

  override def getParser = new JsonParser
}

trait XmlProfile extends CHProfile[NodeSeq] {
  override def getParser = new XmlParser

  override def getMapper = new XmlMapper
}