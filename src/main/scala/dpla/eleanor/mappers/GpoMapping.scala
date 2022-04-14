package dpla.eleanor.mappers

import dpla.eleanor.Schemata
import dpla.eleanor.Schemata.Payload
import dpla.ingestion3.mappers.providers
import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.model.DplaMapData.ZeroToMany

import scala.xml.NodeSeq

class GpoMapping extends providers.GpoMapping {

  override def payloads(data: Document[NodeSeq]): ZeroToMany[Schemata.Payload] = {
    println("mapping payload")
    Seq(Payload())
  }
}

