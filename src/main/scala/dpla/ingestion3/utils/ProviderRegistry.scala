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
    // TODO Uncomment and fixup other provider mappers
    "cdl" -> Register(
      profile = new CdlProfile
    ),
//    "mdl" -> Register(
//      mapper = new MdlExtractor(),
//      harvesterClass = Some(classOf[MdlHarvester]),
//      parser = new JsonParser
//    ),
//    "nara" -> Register(
//      mapper = new NaraExtractorm(),
//      parser = new XmlParser
//    ),
    "pa" -> Register(
      profile = new PaProfile
    ),
    "wi" -> Register(
      profile = new WiProfile
    )
  )

  private def noProfileException(short: String) = {
    val msg = s"No ingestion profile for '$short' found in ProviderRegistry."
    throw new RuntimeException(msg)
  }
}
