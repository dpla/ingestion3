package dpla.eleanor.profiles

import dpla.eleanor.mappers.GpoMapping
import dpla.ingestion3.harvesters.oai.OaiHarvester


/**
  * United States Government Publishing Office (GPO)
  *
  * Uses GPO Ebook Mapping
  */
//noinspection ScalaFileName
class GpoProfile extends XmlProfile {
  type Mapping = GpoMapping

  override def getHarvester: Class[OaiHarvester] = classOf[OaiHarvester]
  override def getMapping = new GpoMapping
}
