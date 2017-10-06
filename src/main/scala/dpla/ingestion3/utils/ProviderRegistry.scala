package dpla.ingestion3.utils

import dpla.ingestion3.harvesters.Harvester
import dpla.ingestion3.harvesters.api.{CdlHarvester, MdlHarvester}
import dpla.ingestion3.mappers.providers._

import scala.util.Try

/**
  * Main entry point for accessing a provider's associated classes based on its
  * short name.
  *
  * @example ProviderRegistry.lookupExtractor("nara") => Success[ classOf[NaraExtractor] ]
  *          ProviderRegistry.lookupHarvester("badShortName") => Failure
  */
object ProviderRegistry {

  /**
    * Get a provider's Extractor class given its short name.
    * @param short: String provider short name
    * @return provider's Extractor class as Success/Failure
    * @throws RuntimeException if short name is not recognized or Extractor class
    *                          is not found
    */
  def lookupExtractorClass(short: String): Try[Class[_ <: Extractor]] = Try {
    registry.getOrElse(short, noExtractorException(short))
      .extractorClass.getOrElse(noExtractorException(short))
  }

  /**
    * Get a provider's Harvester class given its short name.
    * @param short: String provider short name
    * @return provider's Harvester class as Success/Failure
    * @throws RuntimeException if short name is not recognized or Harvester class
    *                          is not found
    */
  def lookupHarvesterClass(short: String): Try[Class[_ <: Harvester]] = Try {
    registry.getOrElse(short, noHarvesterException(short))
      .harvesterClass.getOrElse(noHarvesterException(short))
  }

  private case class Register(extractorClass: Option[Class[_ <: Extractor]] = None,
                              harvesterClass: Option[Class[_ <: Harvester]] = None)

  private val registry: Map[String, Register] = Map(
    "cdl" -> Register(
      Some(classOf[CdlExtractor]),
      Some(classOf[CdlHarvester])
    ),
    "mdl" -> Register(
      Some(classOf[MdlExtractor]),
      Some(classOf[MdlHarvester])
    ),
    "nara" -> Register(
      Some(classOf[NaraExtractor])
    ),
    "pa" -> Register(
      Some(classOf[PaExtractor])
    ),
    "wi" -> Register(
      Some(classOf[WiExtractor])
    )
  )

  private def noExtractorException(short: String) = {
    val msg = s"No Extractor class for '$short' found in ProviderRegistry."
    throw new RuntimeException(msg)
  }

  private def noHarvesterException(short: String) = {
    val msg = s"No Harvester class for '$short' found in ProviderRegistry."
    throw new RuntimeException(msg)
  }
}
