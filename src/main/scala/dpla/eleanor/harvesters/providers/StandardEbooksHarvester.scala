package dpla.eleanor.harvesters.providers

import java.sql.Timestamp

import dpla.eleanor.Schemata
import dpla.eleanor.Schemata.{MetadataType, Payload, SourceUri}
import dpla.eleanor.harvesters.opds1.Opds1Harvester

import scala.xml.Node

/**
  *
  */
class StandardEbooksHarvester(timestamp: Timestamp,
                              source: SourceUri,
                              sourceName: String,
                              metadataType: MetadataType)
  extends Opds1Harvester(timestamp, source, sourceName, metadataType) {

  /**
    * Override getPayload because StandardEbooks gives relative URL path in link properties
    *
    * @param record
    * @return
    */
  override def getPayloads(record: Node): Seq[Schemata.Payload] = {
      val links = for (link <- record \ "link")
        yield Link(link \@ "href", link \@ "rel", link \@ "title", link \@ "type")
      links.collect {
        // collects *all* top level <link> properties, can be more restrictive if required
        case Link(url, _, _, _) => Payload(url = s"https://standardebooks.org$url")
      }
    }
}
