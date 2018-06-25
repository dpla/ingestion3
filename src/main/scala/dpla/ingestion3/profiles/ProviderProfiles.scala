package dpla.ingestion3.profiles

import dpla.ingestion3.harvesters.Harvester
import dpla.ingestion3.harvesters.api.{CdlHarvester, LocHarvester}
import dpla.ingestion3.harvesters.file.{NaraFileHarvester, P2PFileHarvester}
import dpla.ingestion3.harvesters.oai.OaiHarvester
import dpla.ingestion3.mappers.providers._

/**
  * California Digital Library
  */
class CdlProfile extends JsonProfile {
  type Mapping = CdlMapping

  override def getHarvester = classOf[CdlHarvester]
  override def getMapping = new CdlMapping
}

/**
  * District Digital
  */
class DcProfile extends XmlProfile {
  type Mapping = DcMapping

  override def getHarvester = classOf[OaiHarvester]
  override def getMapping = new DcMapping
}

/**
  * Library of Congress
  */
class LocProfile extends JsonProfile {
  type Mapping = LcMapping

  override def getHarvester = classOf[LocHarvester]
  override def getMapping = new LcMapping
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
  * National Archives and Records Administration
  */
class NaraProfile extends XmlProfile {
  type Mapping = NaraMapping

  override def getHarvester: Class[_ <: Harvester] = classOf[NaraFileHarvester]
  override def getMapping = new NaraMapping
}

/**
  * Ohio Hub
  */
class OhioProfile extends XmlProfile {
  type Mapping = OhioMapping

  override def getHarvester = classOf[OaiHarvester]
  override def getMapping = new OhioMapping
}

/**
  * P2P (Plains2Peaks)
  * - Colorado and Wyoming
  */
class P2PProfile extends JsonProfile {
  type Mapping = CdlMapping // FIXME Placeholder

  override def getHarvester = classOf[P2PFileHarvester]
  override def getMapping = new CdlMapping
}
/**
  * Pennsylvania Hub
  */
class PaProfile extends XmlProfile {
  type Mapping = PaMapping

  override def getHarvester = classOf[OaiHarvester]
  override def getMapping = new PaMapping
}

/**
  * Recollection Wisconsin
  */
class WiProfile extends XmlProfile {
  type Mapping = WiMapping

  override def getHarvester = ???
  override def getMapping = new WiMapping
}