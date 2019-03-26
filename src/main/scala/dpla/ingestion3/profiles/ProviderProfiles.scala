package dpla.ingestion3.profiles

import dpla.ingestion3.harvesters.Harvester
import dpla.ingestion3.harvesters.api._
import dpla.ingestion3.harvesters.file._
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
  * Connecticut Digital Library
  */
class CtProfile extends XmlProfile {
  type Mapping = CtMapping

  override def getHarvester = classOf[VaFileHarvester] // CT reuses the VA zipped XML file harvester
  override def getMapping = new CtMapping
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
  * Empire State Digital Network
  */
class EsdnProfile extends XmlProfile {
  type Mapping = EsdnMapping

  override def getHarvester = classOf[OaiHarvester]
  override def getMapping = new EsdnMapping
}

/**
  * Sunshine State Digital Network
  */
class FlProfile extends JsonProfile {
  type Mapping = FlMapping

  override def getHarvester = classOf[FlFileHarvester]
  override def getMapping = new FlMapping
}

/**
  * J. Paul Getty Trust
  */
class GettyProfile extends XmlProfile {
  type Mapping = GettyMapping

  override def getHarvester = classOf[GettyHarvester]
  override def getMapping = new GettyMapping
}

/**
  * Digital Library of Georgia
  */
class DlgProfile extends JsonProfile {
  type Mapping = DlgMapping // FIXME placeholder only

  override def getHarvester = classOf[DlgHarvester]
  override def getMapping = new DlgMapping // FIXME placeholder only
}

/**
  * Internet Archive
  */
class IaProfile extends JsonProfile {
  type Mapping = IaMapping

  override def getHarvester = classOf[IaHarvester]
  override def getMapping = new IaMapping
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

  override def getHarvester: Class[_ <: Harvester] = classOf[MdlHarvester]
  override def getMapping = new MdlMapping
}

/**
  * Michigan Service Hub
  */
class MiProfile extends XmlProfile {
  type Mapping = MichiganMapping

  override def getHarvester: Class[_ <: Harvester] = classOf[OaiHarvester]
  override def getMapping = new MichiganMapping
}

/**
  * Missouri
  */
class MoProfile extends JsonProfile {
  type Mapping = MoMapping

  override def getHarvester: Class[_ <: Harvester] = classOf[MoFileHarvester]
  override def getMapping = new MoMapping
}

/**
  * (Montana) Big Sky Digital Network
  */
class MtProfile extends XmlProfile {
  type Mapping = MtMapping

  override def getHarvester: Class[_ <: Harvester] = classOf[OaiHarvester]
  override def getMapping = new MtMapping
}

/**
  * Mountain West Digital Library
  */
class MwdlProfile extends XmlProfile {
  type Mapping = MwdlMapping

  override def getHarvester: Class[_ <: Harvester] = classOf[MwdlHarvester]
  override def getMapping = new MwdlMapping
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
  * Oklahoma Hub
  */
class OklahomaProfile extends XmlProfile {
  type Mapping = OklahomaMapping

  override def getHarvester = classOf[OaiHarvester]
  override def getMapping = new OklahomaMapping
}

/**
  * P2P (Plains2Peaks)
  * - Colorado and Wyoming
  */
class P2PProfile extends XmlProfile {
  type Mapping = P2PMapping

  override def getHarvester = classOf[OaiHarvester]
  override def getMapping = new P2PMapping
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
  * South Dakota
  */
class SdProfile extends JsonProfile {
  type Mapping = SdMapping

  override def getHarvester = classOf[MdlHarvester]
  override def getMapping = new SdMapping
}

/**
  * Digital Virginias
  */
class VirginiasProfile extends XmlProfile {
  type Mapping = VirginiasMapping

  override def getHarvester = classOf[VaFileHarvester]
  override def getMapping = new VirginiasMapping
}

/**
  * Vermont (Green Mountain Digital Archives)
  */
class VtProfile extends XmlProfile {
  type Mapping = VtMapping

  override def getHarvester = classOf[VtFileHarvester]
  override def getMapping = new VtMapping
}

/**
  * Recollection Wisconsin
  */
class WiProfile extends XmlProfile {
  type Mapping = WiMapping

  override def getHarvester = classOf[OaiHarvester]
  override def getMapping = new WiMapping
}