package dpla.ingestion3.profiles

import dpla.ingestion3.harvesters.Harvester
import dpla.ingestion3.harvesters.api._
import dpla.ingestion3.harvesters.file._
import dpla.ingestion3.harvesters.oai.OaiHarvester
import dpla.ingestion3.mappers.providers._

/**
  * Biodiversity Heritage Library
  */
class BhlProfile extends XmlProfile {
  type Mapping = CdlMapping

  override def getHarvester = classOf[OaiHarvester]
  override def getMapping = new BhlMapping
}

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

  override def getHarvester = classOf[DlgFileHarvester]
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
  type Mapping = DlgMapping

  override def getHarvester = classOf[DlgFileHarvester]
  override def getMapping = new DlgMapping
}

/**
  * HathiTrust
  */
class HathiProfile extends XmlProfile {
  type Mapping = IllinoisMapping // TODO: CHANGEME

  override def getHarvester = classOf[HathiFileHarvester]
  override def getMapping = new IllinoisMapping // TODO: CHANGEME
}

/**
  * Illinois Digital Heritage Hub
  */
class IlProfile extends XmlProfile {
  type Mapping = IllinoisMapping

  override def getHarvester = classOf[OaiHarvester]
  override def getMapping = new IllinoisMapping
}

/**
  * Indiana Memory
  */
class InProfile extends XmlProfile {
  type Mapping = InMapping

  override def getHarvester = classOf[OaiHarvester]
  override def getMapping = new InMapping
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

  override def getHarvester = classOf[LcCsvFileHarvester]
  override def getMapping = new LcMapping
}

/**
  * Massachusetts - Digital Commonwealth
  */
class MaProfile extends XmlProfile {
  type Mapping = MaMapping

  override def getHarvester: Class[_ <: Harvester] = classOf[OaiHarvester]
  override def getMapping = new MaMapping
}


/**
  * Digital Maryland
  */
class MarylandProfile extends XmlProfile {
  type Mapping = MarylandMapping

  override def getHarvester: Class[_ <: Harvester] = classOf[MdlHarvester]
  override def getMapping = new MarylandMapping
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
  * Missouri Hub
  */
class MissouriProfile extends JsonProfile {
  type Mapping = MissouriMapping

  override def getHarvester: Class[_ <: Harvester] = classOf[MissouriFileHarvester]
  override def getMapping = new MissouriMapping
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
  * North Carolina
  */
class NcProfile extends XmlProfile {
  type Mapping = NcMapping

  override def getHarvester = classOf[OaiHarvester]
  override def getMapping = new NcMapping
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
  * Tennessee Digital Library
  */
class TnProfile extends XmlProfile {
  type Mapping = TnMapping

  override def getHarvester = classOf[OaiHarvester]

  override def getMapping = new TnMapping
}

/**
  * Smithsonian Institution
  */
class SiProfile extends XmlProfile {
  type Mapping = SiMapping

  override def getHarvester = classOf[SiFileHarvester]
  override def getMapping = new SiMapping
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