package dpla.ingestion3.utils

import dpla.ingestion3.profiles._

import scala.util.Try

/**
  * Main entry point for accessing a provider's associated classes based on its
  * short name.
  *
  */
object CHProviderRegistry {
  /**
    *
    * @param short Provider shortname
    * @return
    */
  def lookupRegister(short: String) = Try {
    registry.getOrElse(short, noProfileException(short))
  }

  /**
    * Get a providers ingestion profile
    *
    * @param short Provider shortname
    * @return
    */
  def lookupProfile(short: String) = Try {
    registry.getOrElse(short, noProfileException(short)).profile
  }

  /**
    *
    * @param short
    * @return
    */
  def lookupHarvesterClass(short: String) = Try {
    registry.getOrElse(short, noProfileException(short)).profile.getHarvester
  }

  case class Register[IngestionProfile] (profile: IngestionProfile)

  private val registry = Map(
    // FIXME Register is redundant here and should be removed

    "artstor" -> Register(profile = new ArtstorProfile),
    "bhl" -> Register(profile = new BhlProfile),
    "bpl" -> Register(profile = new MaProfile),
    "cdl" -> Register(profile = new CdlProfile),
    "community-webs" -> Register(profile = new CommunityWebsProfile),
    "ct" -> Register(profile = new CtProfile),
    "dc" -> Register(profile = new DcProfile),
    "esdn" -> Register(profile = new EsdnProfile),
    "florida" -> Register(profile = new FlProfile),
    "georgia" -> Register(profile = new DlgProfile),
    "getty" -> Register(profile = new GettyProfile),
    "gpo" -> Register(profile = new GpoProfile),
    "harvard" -> Register(profile = new HarvardProfile),
    "hathi" -> Register(profile = new HathiProfile),
    "heartland" -> Register(profile = new HeartlandProfile),
    "ia" -> Register(profile = new IaProfile),
    "il" -> Register(profile = new IlProfile),
    "indiana" -> Register(profile = new InProfile),
    "jhn" -> Register(profile = new JhnProfile), // Onboarding
    "kentucky" -> Register(profile = new KyProfile),
    "lc" -> Register(profile = new LocProfile),
    "maine" -> Register(profile = new MeProfile),
    "maryland" -> Register(profile = new MarylandProfile),
    "mi" -> Register(profile = new MiProfile),
    "minnesota" -> Register(profile = new MdlProfile),
    "mississippi" -> Register(profile = new MississippiProfile), // Onboarding
    "mt" -> Register(profile = new MtProfile),
    "mwdl" -> Register(profile = new MwdlProfile),
    "nara" -> Register(profile = new NaraProfile),
    "digitalnc" -> Register(profile = new NcProfile),
    "njde" -> Register(profile = new NJDEProfile),
    "northwest-heritage" -> Register(profile = new NorthwestHeritageProfile),
    "nypl" -> Register(profile = new NYPLProfile),
    "ohio" -> Register(profile = new OhioProfile),
    "oklahoma" -> Register(profile = new OklahomaProfile),
    "orbis-cascade" -> Register(profile = new OrbisCascadeProfile),
    "p2p" -> Register(profile = new P2PProfile),
    "pa" -> Register(profile = new PaProfile),
    "david_rumsey" -> Register(profile = new RumseyProfile),
    "scdl" -> Register(profile = new ScProfile),
    "sd" -> Register(profile = new SdProfile),
    "smithsonian" -> Register(profile = new SiProfile),
    "texas" -> Register(profile = new TxProfile),
    "tennessee" -> Register(profile = new TnProfile),
    "txdl" -> Register(profile = new TxdlProfile),
    "virginias" -> Register(profile = new VirginiasProfile),
    "vt" -> Register(profile = new VtProfile),
    "washington" -> Register(profile = new WashingtonProfile),
    "wisconsin" -> Register(profile = new WiProfile)
  )

  private def noProfileException(short: String) = {
    val msg = s"No ingestion profile for '$short' found in Cultural Heritage Provider Registry."
    throw new RuntimeException(msg)
  }
}
