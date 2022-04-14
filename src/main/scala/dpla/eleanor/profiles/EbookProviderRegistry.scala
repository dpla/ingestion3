package dpla.eleanor.profiles

import scala.util.Try

/**
  * Main entry point for accessing an Ebook provider's associated classes based on its
  * short name.
  *
  */
object EbookProviderRegistry {
  /**
    *
    * @param short Provider shortname
    * @return
    */
  def lookupRegister(short: String) = Try {
    registry.getOrElse(short, noProfileException(short))
  }

  /**
    * Get a providers ebook profile
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

  case class Register[EbookProfile] (profile: EbookProfile)

  private val registry = Map(
    "http://gpo.gov" -> Register(profile = new GpoProfile)
  )

  private def noProfileException(short: String) = {
    val msg = s"No ebook profile for '$short' found in Ebook provider registry."
    throw new RuntimeException(msg)
  }
}
