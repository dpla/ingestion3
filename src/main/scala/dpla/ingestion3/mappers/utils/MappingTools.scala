package dpla.ingestion3.mappers.utils

import org.json4s.JValue

import scala.xml.NodeSeq


trait MappingTools[T] {
  type Parser
  type Extractor
  type ParsedRepresentation
}

trait XmlMappingTools extends MappingTools[NodeSeq] {
  override type Parser = XmlParser
  override type Extractor = XmlExtractor
  override type ParsedRepresentation = NodeSeq
}

trait JsonMappingTools extends MappingTools[JValue] {
  override type Parser = JsonParser
  override type Extractor = JsonExtractor
  override type ParsedRepresentation = JValue
}
