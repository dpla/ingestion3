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

    "bhl" -> Register(profile = new BhlProfile),
    "cdl" -> Register(profile = new CdlProfile),
    "ct" -> Register(profile = new CtProfile),
    "dc" -> Register(profile = new DcProfile),
    "esdn" -> Register(profile = new EsdnProfile),
    "florida" -> Register(profile = new FlProfile),
    "georgia" -> Register(profile = new DlgProfile),
    "getty" -> Register(profile = new GettyProfile),
    "gpo" -> Register(profile = new GpoProfile),
    "harvard" -> Register(profile = new HarvardProfile),
    "hathi" -> Register(profile = new HathiProfile),
    "ia" -> Register(profile = new IaProfile),
    "il" -> Register(profile = new IlProfile),
    "indiana" -> Register(profile = new InProfile),
    "lc" -> Register(profile = new LocProfile),
    "bpl" -> Register(profile = new MaProfile),
    "me" -> Register(profile = new MeProfile),
    "maryland" -> Register(profile = new MarylandProfile),
    "mi" -> Register(profile = new MiProfile),
    "minnesota" -> Register(profile = new MdlProfile),
    "missouri" -> Register(profile = new MissouriProfile),
    "mississippi" -> Register(profile = new MississippiProfile),
    "mt" -> Register(profile = new MtProfile),
    "mwdl" -> Register(profile = new MwdlProfile),
    "nara" -> Register(profile = new NaraProfile),
    "nc" -> Register(profile = new NcProfile),
    "njde" -> Register(profile = new NJDEProfile),
    "northwest-heritage" -> Register(profile = new NorthwestHeritageProfile),
    "nypl" -> Register(profile = new NYPLProfile),
    "oklahoma" -> Register(profile = new OklahomaProfile),
    "ohio" -> Register(profile = new OhioProfile),
    "orbis-cascade" -> Register(profile = new OrbisCascadeProfile),
    "p2p" -> Register(profile = new P2PProfile), // plains2peaks
    "pa" -> Register(profile = new PaProfile),
    "sc" -> Register(profile = new ScProfile),
    "rumsey" -> Register(profile = new RumseyProfile),
    "sd" -> Register(profile = new SdProfile),
    "tn" -> Register(profile = new TnProfile),
    "texas" -> Register(profile = new TxProfile),
    "txdl" -> Register(profile = new TxdlProfile),
    "smithsonian" -> Register(profile = new SiProfile),
    "virginias" -> Register(profile = new VirginiasProfile),
    "vt" -> Register(profile = new VtProfile),
    "wisconsin" -> Register(profile = new WiProfile)
  )

  private def noProfileException(short: String) = {
    val msg = s"No ingestion profile for '$short' found in Cultural Heritage Provider Registry."
    throw new RuntimeException(msg)
  }
}
