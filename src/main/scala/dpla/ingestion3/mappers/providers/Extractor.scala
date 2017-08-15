package dpla.ingestion3.mappers.providers

import java.net.URI

import dpla.ingestion3.model.{DplaMap, DplaMapData, DplaMapError, EdmAgent}
import dpla.ingestion3.utils.Utils
import org.apache.pig.data.utils.MethodHelper.NotImplemented

import scala.util.{Failure, Success, Try}

/**
  * Interface that all provider extractors implement.
  */

trait Extractor {
  // Base item uri
  private val baseItemUri = "http://dp.la/api/items/"

  def build(): DplaMap
  def agent: EdmAgent
  /**
    * Build the base ID to be hashed. Implemented per provider
    *
    * @return String
    * @throws NotImplemented
    */
  def getProviderBaseId(): Option[String]

  /**
    * Hashes the base ID
    *
    * @return MD5 hash of the base ID
    */
  protected def mintDplaId(): String = Utils.generateMd5(getProviderBaseId())

  /**
    * Builds the item URI
    *
    * @return URI
    */
  protected def mintDplaItemUri(): URI = new URI(s"${baseItemUri}${mintDplaId()}")

  /**
    * Implicit method used to convert Try[DplaMap] objects to either
    * DplaMapData or DplaMapError objects depending on whether the mapping
    * task was performed without errors.
    *
    * @param data The result from Extractor.build()
    * @return DplaMapData or DplaMapError
    */
  implicit def DplaDataToDataOrError(data: Try[DplaMap]) = data match {
    case Success(s) => s
      // TODO If the record ID is not available it would be helpful to dump the original record out here
    case Failure(f) => DplaMapError(f.getMessage + s" for record " +
      s"${getProviderBaseId().getOrElse(s"ID not available! !! TODO !! Dumping record...\n")}")
  }
}