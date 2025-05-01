package dpla.ingestion3.utils

import dpla.ingestion3.harvesters.Harvester
import dpla.ingestion3.profiles._
import org.json4s.JValue

import scala.util.Try
import scala.xml.NodeSeq

/** Main entry point for accessing a provider's associated classes based on its
  * short name.
  */
object CHProviderRegistry {

  def lookupProfile(short: String): Try[CHProfile[_ >: NodeSeq with JValue]] = Try {
    registry.getOrElse(short, noProfileException(short))
  }

  def lookupHarvesterClass(short: String): Try[Class[_ <: Harvester]] = Try {
    registry.getOrElse(short, noProfileException(short)).getHarvester
  }

  private val registry = Map(
    "artstor" -> new IthakaProfile,
    "ithaka" -> new IthakaProfile,
    "bhl" -> new BhlProfile,
    "bpl" -> new MaProfile,
    "community-webs" -> new CommunityWebsProfile,
    "ct" -> new CtProfile,
    "dc" -> new DcProfile,
    "florida" -> new FlProfile,
    "georgia" -> new DlgProfile,
    "getty" -> new GettyProfile,
    "gpo" -> new GpoProfile,
    "harvard" -> new HarvardProfile,
    "hathi" -> new HathiProfile,
    "heartland" -> new HeartlandProfile,
    "ia" -> new IaProfile,
    "il" -> new IlProfile,
    "indiana" -> new InProfile,
    "jhn" -> new JhnProfile,
    "lc" -> new LocProfile,
    "maryland" -> new MarylandProfile,
    "mi" -> new MiProfile,
    "minnesota" -> new MdlProfile,
    "mississippi" -> new MississippiProfile,
    "mt" -> new MtProfile,
    "mwdl" -> new MwdlProfile,
    "nara" -> new NaraProfile,
    "digitalnc" -> new NcProfile,
    "njde" -> new NJDEProfile,
    "northwest-heritage" -> new NorthwestHeritageProfile,
    "nypl" -> new NYPLProfile,
    "ohio" -> new OhioProfile,
    "oklahoma" -> new OklahomaProfile,
    "p2p" -> new P2PProfile,
    "pa" -> new PaProfile,
    "david-rumsey" -> new RumseyProfile,
    "scdl" -> new ScProfile,
    "sd" -> new SdProfile,
    "smithsonian" -> new SiProfile,
    "texas" -> new TxProfile,
    "tennessee" -> new TnProfile,
    "txdl" -> new TxdlProfile,
    "virginias" -> new VirginiasProfile,
    "vt" -> new VtProfile,
    "wisconsin" -> new WiProfile
  )

  private def noProfileException(short: String): Nothing = {
    val msg =
      s"No ingestion profile for '$short' found in Cultural Heritage Provider Registry."
    throw new RuntimeException(msg)
  }
}
