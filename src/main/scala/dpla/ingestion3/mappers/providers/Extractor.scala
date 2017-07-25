package dpla.ingestion3.mappers.providers

import java.net.URI

import dpla.ingestion3.model.{DplaMapData, EdmAgent}
import dpla.ingestion3.utils.Utils
import org.apache.pig.data.utils.MethodHelper.NotImplemented

/**
  * Interface that all provider extractors implement.
  */

trait Extractor {
  // Base item uri
  private val baseItemUri = "http://dp.la/api/items/"

  def build(): DplaMapData
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
}