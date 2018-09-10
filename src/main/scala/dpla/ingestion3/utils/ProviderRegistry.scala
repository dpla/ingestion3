package dpla.ingestion3.utils

import dpla.ingestion3.profiles._

import scala.util.Try

/**
  * Main entry point for accessing a provider's associated classes based on its
  * short name.
  *
  */
object ProviderRegistry {
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

    "cdl" -> Register(profile = new CdlProfile),
    "dc" -> Register(profile = new DcProfile),
    "esdn" -> Register(profile = new EsdnProfile),
    "ia" -> Register(profile = new IaProfile),
    "loc" -> Register(profile = new LocProfile),
    "mdl" -> Register(profile = new MdlProfile),
    "mt" -> Register(profile = new MtProfile),
    "nara" -> Register(profile = new NaraProfile),
    "ohio" -> Register(profile = new OhioProfile),
    "p2p" -> Register(profile = new P2PProfile), // plains2peaks
    "pa" -> Register(profile = new PaProfile),
    "sd" -> Register(profile = new SdProfile),
    "wi" -> Register(profile = new WiProfile)
  )

  private def noProfileException(short: String) = {
    val msg = s"No ingestion profile for '$short' found in ProviderRegistry."
    throw new RuntimeException(msg)
  }
}
