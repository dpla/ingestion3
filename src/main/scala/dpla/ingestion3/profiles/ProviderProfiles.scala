package dpla.ingestion3.profiles

import dpla.ingestion3.harvesters.Harvester
import dpla.ingestion3.harvesters.api.CdlHarvester
import dpla.ingestion3.harvesters.oai.OaiHarvester
import dpla.ingestion3.mappers.providers._

/**
  * California Digital Library
  */
class CdlProfile extends JsonProfile {
  type Mapping = CdlMapping

  // FIXME
  override def getHarvester = classOf[CdlHarvester]
  override def getMapping = new CdlMapping

}

/**
  * Minnesota Digital Library
  */
class MdlProfile extends JsonProfile {
  type Mapping = MdlMapping

  override def getHarvester: Class[_ <: Harvester] = ???
  override def getMapping = new MdlMapping
}

/**
  * Pennsylvania Hub
  */
class PaProfile extends XmlProfile {
  type Mapping = PaMapping

  // FIXME
  override def getHarvester = classOf[OaiHarvester]
  override def getMapping = new PaMapping
}

/**
  *
  */
class WiProfile extends XmlProfile {
  type Mapping = WiMapping

  override def getHarvester = ???
  override def getMapping = new WiMapping
}