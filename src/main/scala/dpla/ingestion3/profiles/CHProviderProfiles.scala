package dpla.ingestion3.profiles

import dpla.ingestion3.harvesters.Harvester
import dpla.ingestion3.harvesters.api._
import dpla.ingestion3.harvesters.file._
import dpla.ingestion3.harvesters.oai.{LocalOaiHarvester, OaiHarvester}
import dpla.ingestion3.mappers.providers._

class ArtstorProfile extends JsonProfile {
  type Mapping = CdlMapping

  override def getHarvester: Class[_ <: Harvester] = classOf[LocalOaiHarvester]
  override def getMapping = new ArtstorRetconMapping
}

/** Biodiversity Heritage Library
 */
class BhlProfile extends XmlProfile {
  type Mapping = BhlMapping

  override def getHarvester: Class[_ <: Harvester] = classOf[LocalOaiHarvester]
  override def getMapping = new BhlMapping
}

/** California Digital Library
 * Currently down. They rebuilt on new tech and there's no current endpoint available.
 */
class CdlProfile extends JsonProfile {
  type Mapping = CdlMapping

  override def getHarvester: Class[_ <: Harvester] = classOf[CdlHarvester]
  override def getMapping = new CdlMapping
}

/** Connecticut Digital Library
 * Currently down. They're migrating to a new repository.
 */
class CtProfile extends XmlProfile {
  type Mapping = CtMapping

  override def getHarvester: Class[_ <: Harvester] =
    classOf[VaFileHarvester] // CT reuses the VA zipped XML file harvester
  override def getMapping = new CtMapping
}

/** Community Webs
 */
class CommunityWebsProfile extends JsonProfile {
  type Mapping = CommunityWebsMapping

  override def getHarvester: Class[_ <: Harvester] =
    classOf[CommunityWebsHarvester]
  override def getMapping = new CommunityWebsMapping
}

/** District Digital
 * Server not around at the moment...
 */
class DcProfile extends XmlProfile {
  type Mapping = DcMapping

  override def getHarvester: Class[_ <: Harvester] = classOf[LocalOaiHarvester]
  override def getMapping = new DcMapping
}

/** Empire State Digital Network
 *  Dead hub.
 */
class EsdnProfile extends XmlProfile {
  type Mapping = EsdnMapping

  override def getHarvester: Class[_ <: Harvester] = classOf[LocalOaiHarvester]
  override def getMapping = new EsdnMapping
}

/** Sunshine State Digital Network
 */
class FlProfile extends JsonProfile {
  type Mapping = FlMapping

  override def getHarvester: Class[_ <: Harvester] = classOf[FlFileHarvester]
  override def getMapping = new FlMapping
}

/** J. Paul Getty Trust
 *  Currently down; Primo errors.
 */
class GettyProfile extends JsonProfile {
  type Mapping = GettyMapping

  override def getHarvester: Class[_ <: Harvester] = classOf[GettyHarvester]
  override def getMapping = new GettyMapping
}

/** United States Government Publishing Office (GPO)
 */
class GpoProfile extends XmlProfile {
  type Mapping = GpoMapping

  override def getHarvester: Class[_ <: Harvester] = classOf[LocalOaiHarvester]
  override def getMapping = new GpoMapping
}

/** Digital Library of Georgia
 */
class DlgProfile extends JsonProfile {
  type Mapping = DlgMapping

  override def getHarvester: Class[_ <: Harvester] = classOf[DlgFileHarvester]
  override def getMapping = new DlgMapping
}

/** Harvard Library
 */
class HarvardProfile extends XmlProfile {
  type Mapping = HarvardMapping

  override def getHarvester: Class[_ <: Harvester] = classOf[LocalOaiHarvester]
  override def getMapping = new HarvardMapping
}

/** HathiTrust
 *  Currently needs rebuilding around their export formats.
 */
class HathiProfile extends XmlProfile {
  type Mapping = HathiMapping

  override def getHarvester: Class[_ <: Harvester] =
    classOf[HathiFileHarvester]
  override def getMapping = new HathiMapping
}

/** Illinois Digital Heritage Hub
 */
class IlProfile extends XmlProfile {
  type Mapping = IllinoisMapping

  override def getHarvester: Class[_ <: Harvester] = classOf[LocalOaiHarvester]
  override def getMapping = new IllinoisMapping
}

/** Indiana Memory
 */
class InProfile extends XmlProfile {
  type Mapping = InMapping

  override def getHarvester: Class[_ <: Harvester] = classOf[LocalOaiHarvester]
  override def getMapping = new InMapping
}

/** Jewish Heritage Network
 */
class JhnProfile extends XmlProfile {
  override def getHarvester: Class[_ <: Harvester] = classOf[LocalOaiHarvester]
  override def getMapping = new JhnMapping
}

/** Internet Archive
 */
class IaProfile extends JsonProfile {
  type Mapping = IaMapping

  override def getHarvester: Class[_ <: Harvester] = classOf[IaHarvester]
  override def getMapping = new IaMapping
}

/** Kentucky
 * Dead hub. Profile based on migration from ingestion1
 */
class KyProfile extends JsonProfile {
  type Mapping = KentuckyRetconMapping

  override def getHarvester: Class[_ <: Harvester] =
    classOf[DplaJsonlFileHarvester]
  override def getMapping = new KentuckyRetconMapping
}

/** Library of Congress
 */
class LocProfile extends JsonProfile {
  type Mapping = LcRetconMapping

  override def getHarvester: Class[_ <: Harvester] =
    classOf[DplaJsonlFileHarvester]
  override def getMapping = new LcRetconMapping
}

/** Digital Maine
 * Hub has gone out of comms. Profile based on migration from ingestion1
 */
class MeProfile extends JsonProfile {
  type Mapping = MaineRetconMapping

  override def getHarvester: Class[_ <: Harvester] =
    classOf[DplaJsonlFileHarvester]
  override def getMapping = new MaineRetconMapping
}

/** Massachusetts - Digital Commonwealth
 */
class MaProfile extends XmlProfile {
  type Mapping = MaMapping

  override def getHarvester: Class[_ <: Harvester] = classOf[LocalOaiHarvester]
  override def getMapping = new MaMapping
}

/** Digital Maryland
 */
class MarylandProfile extends XmlProfile {
  type Mapping = MarylandMapping

  override def getHarvester: Class[_ <: Harvester] = classOf[LocalOaiHarvester]
  override def getMapping = new MarylandMapping
}

/** Minnesota Digital Library
 */
class MdlProfile extends JsonProfile {
  type Mapping = MdlMapping

  override def getHarvester: Class[_ <: Harvester] = classOf[MdlHarvester]
  override def getMapping = new MdlMapping
}

/** Michigan Service Hub
 */
class MiProfile extends XmlProfile {
  type Mapping = MichiganMapping

  override def getHarvester: Class[_ <: Harvester] = classOf[LocalOaiHarvester]
  override def getMapping = new MichiganMapping
}

/** Mississippi Hub
 */
class MississippiProfile extends JsonProfile {
  type Mapping = MississippiMapping

  override def getHarvester: Class[_ <: Harvester] =
    classOf[MississippiHarvester]
  override def getMapping = new MississippiMapping
}

/** Heartland Hub
 */
class HeartlandProfile extends JsonProfile {
  type Mapping = HeartlandMapping

  override def getHarvester: Class[_ <: Harvester] =
    classOf[HeartlandFileHarvester]
  override def getMapping = new HeartlandMapping
}

/** (Montana) Big Sky Digital Network
 */
class MtProfile extends XmlProfile {
  type Mapping = MtMapping

  override def getHarvester: Class[_ <: Harvester] = classOf[LocalOaiHarvester]
  override def getMapping = new MtMapping
}

/** Mountain West Digital Library
 */
class MwdlProfile extends XmlProfile {
  type Mapping = MwdlMapping

  override def getHarvester: Class[_ <: Harvester] = classOf[MwdlHarvester]
  override def getMapping = new MwdlMapping
}

/** National Archives and Records Administration
 */
class NaraProfile extends XmlProfile {
  type Mapping = NaraMapping

  override def getHarvester: Class[_ <: Harvester] = classOf[NaraFileHarvester]
  override def getMapping = new NaraMapping
}

/** NJ/DE Digital Collective
 */
class NJDEProfile extends XmlProfile {
  type Mapping = NJDEMapping

  override def getHarvester: Class[_ <: Harvester] = classOf[LocalOaiHarvester]
  override def getMapping = new NJDEMapping
}

/** North Carolina
 */
class NcProfile extends XmlProfile {
  type Mapping = NcMapping

  override def getHarvester: Class[_ <: Harvester] = classOf[LocalOaiHarvester]
  override def getMapping = new NcMapping
}

/** Northwest Digital Heritage
 */
class NorthwestHeritageProfile extends XmlProfile {
  type Mapping = NorthwestHeritageMapping

  override def getHarvester: Class[_ <: Harvester] = classOf[OaiFileHarvester]
  override def getMapping = new NorthwestHeritageMapping
}

/** New York Public Library
 */
class NYPLProfile extends NyplCHProfile {
  type Mapping = NyplMapping

  override def getHarvester: Class[_ <: Harvester] =
    classOf[NYPLFileHarvester]
  override def getMapping = new NyplMapping
}

/** Ohio Hub
 */
class OhioProfile extends XmlProfile {
  type Mapping = OhioMapping

  override def getHarvester: Class[_ <: Harvester] = classOf[LocalOaiHarvester]
  override def getMapping = new OhioMapping
}

/** Orbis-Cascade
 * Dead hub.
 */
class OrbisCascadeProfile extends XmlProfile {
  type Mapping = OrbisCascadeMapping

  override def getHarvester: Class[_ <: Harvester] =
    classOf[OrbisCascadeFileHarvester]
  override def getMapping = new OrbisCascadeMapping
}

/** Oklahoma Hub
 */
class OklahomaProfile extends XmlProfile {
  type Mapping = OklahomaMapping

  override def getHarvester: Class[_ <: Harvester] = classOf[LocalOaiHarvester]
  override def getMapping = new OklahomaMapping
}

/** P2P (Plains2Peaks)
 *   - Colorado and Wyoming
 */
class P2PProfile extends XmlProfile {
  type Mapping = P2PMapping

  override def getHarvester: Class[_ <: Harvester] = classOf[LocalOaiHarvester]
  override def getMapping = new P2PMapping
}

/** Pennsylvania Hub
 */
class PaProfile extends XmlProfile {
  type Mapping = PaMapping

  override def getHarvester: Class[_ <: Harvester] = classOf[LocalOaiHarvester]
  override def getMapping = new PaMapping
}

/** David Rumsey Map Collection
 */
class RumseyProfile extends XmlProfile {
  type Mapping = RumseyMapping

  override def getHarvester: Class[_ <: Harvester] = classOf[LocalOaiHarvester]
  override def getMapping = new RumseyMapping
}

/** South Carolina Digital Library
 */
class ScProfile extends XmlProfile {
  type Mapping = ScMapping

  override def getHarvester: Class[_ <: Harvester] = classOf[LocalOaiHarvester]
  override def getMapping = new ScMapping
}

/** South Dakota
 */
class SdProfile extends JsonProfile {
  type Mapping = SdMapping

  override def getHarvester: Class[_ <: Harvester] = classOf[MdlHarvester]
  override def getMapping = new SdMapping
}

/** Tennessee Digital Library
 */
class TnProfile extends XmlProfile {
  type Mapping = TnMapping

  override def getHarvester: Class[_ <: Harvester] = classOf[LocalOaiHarvester]
  override def getMapping = new TnMapping
}

/** The Portal to Texas History
 */
class TxProfile extends XmlProfile {
  type Mapping = TnMapping

  override def getHarvester: Class[_ <: Harvester] = classOf[LocalOaiHarvester]
  override def getMapping = new TxMapping
}

/** Texas Digital Library (aka Texas 2)
 */
class TxdlProfile extends XmlProfile {
  type Mapping = TxdlMapping

  override def getHarvester: Class[_ <: Harvester] = classOf[OaiFileHarvester]
  override def getMapping = new TxdlMapping
}

/** Smithsonian Institution
 */
class SiProfile extends XmlProfile {
  type Mapping = SiMapping

  override def getHarvester: Class[_ <: Harvester] = classOf[SiFileHarvester]
  override def getMapping = new SiMapping
}

/** Digital Virginias
 * File obtained by going to the github repo and downloading it as a zip
 */
class VirginiasProfile extends XmlProfile {
  type Mapping = VirginiasMapping

  override def getHarvester: Class[_ <: Harvester] = classOf[VaFileHarvester]
  override def getMapping = new VirginiasMapping
}

/** Vermont (Green Mountain Digital Archives)
 */
class VtProfile extends XmlProfile {
  type Mapping = VtMapping

  override def getHarvester: Class[_ <: Harvester] = classOf[VtFileHarvester]
  override def getMapping = new VtMapping
}

/** University of Washington
 * Seemingly down. Profile based on migration from ingestion1
 */
class WashingtonProfile extends JsonProfile {
  type Mapping = WashingtonRetconMapping

  override def getHarvester: Class[_ <: Harvester] =
    classOf[DplaJsonlFileHarvester]
  override def getMapping = new WashingtonRetconMapping
}

/** Recollection Wisconsin
 */
class WiProfile extends XmlProfile {
  type Mapping = WiMapping

  override def getHarvester: Class[_ <: Harvester] = classOf[LocalOaiHarvester]
  override def getMapping = new WiMapping
}