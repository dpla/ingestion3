package dpla.ingestion3.mappers.utils

import org.json4s.JValue

import scala.xml.NodeSeq

trait XmlMapping extends Mapping[NodeSeq]

trait JsonMapping extends Mapping[JValue]