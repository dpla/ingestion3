package dpla.ingestion3.utils

import dpla.ingestion3.harvesters.Harvester
import dpla.ingestion3.mappers.XmlParser
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
    *
    * @param short
    * @return
    */
  def lookupRegister(short: String) = Try {
    registry.getOrElse(short, noExtractorException(short))
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

  case class Register[T, Mapper[T], Parser[T]](
                                  mapper: Mapper[T],
                                  harvesterClass: Option[Class[_ <: Harvester]] = None,
                                  parser: Parser[T]
                                )

  private val registry = Map(
    // TODO Uncomment and fixup other provider mappers
//    "cdl" -> Register(
//      mapper = new CdlExtractor,
//      harvesterClass = Some(classOf[CdlHarvester]),
//      parser = new JsonParser
//    ),
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
      mapper = new PaExtractor,
      parser = new XmlParser
    )
//    ,"wi" -> Register(
//      mapper = new WiExtractor,
//      parser = new XmlParser
//    )
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
