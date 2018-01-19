package dpla.ingestion3.mappers.utils

import java.net.URI

import org.apache.commons.codec.digest.DigestUtils

import scala.util.{Failure, Success, Try}

trait IdMinter[T] {

  // Base item uri
  private val baseDplaItemUri = "http://dp.la/api/items/"

  /**
    * Does the provider use a prefix (typically their provider shortname/abbreviation) to
    * salt the base identifier?
    *
    * @return Boolean
    */
  def useProviderName: Boolean

  /**
    * Extract the record's "persistent" identifier. Implementations should raise
    * an Exception if no ID can be extracted
    *
    * @return String Record identifier
    * @throws Exception If ID can not be extracted
    */
  def getProviderId(implicit data: T): String

  /**
    * The provider's shortname abbreviation which is the value used to salt the
    * local identifier with when minting the DPLA identifier. Not all providers
    * use this "shortname--id" ID construction.
    *
    * @see buildProviderBaseId
    * @return String Provider shortname
    */
  def getProviderName: String = ""

  /**
    * Builds the ID to be hashed by either concatenating the provider's
    * abbreviated name and the persistent identifier for the record with
    * a double dash `--` or only the persistent identifier.
    *
    * Some providers use a name prefix and some do not. Please see Hub
    * mapping documentation and individual Extractor classes.
    *
    * ############################################################
    * # WARNING DO NOT CHANGE UNLESS YOU KNOW WHAT YOU ARE DOING #
    * ############################################################
    *
    * @return String
    */
  protected def buildProviderBaseId()(implicit data: T): String = {

    def idErrorMsg(): String = {
      s"Unable to mint ID given values of:\n" +
        s"useProviderName: $useProviderName\n" +
        s"getProviderName: $getProviderName\n" +
        s"getProviderId: $getProviderId\n"
    }

    Try {
      if (useProviderName) {
        s"$getProviderName--$getProviderId"
      } else {
        getProviderId
      }
    } match {
      case Success(id) => id
      case Failure(_) => throw new RuntimeException(idErrorMsg())
    }
  }

  /**
    * Hashes the base ID
    *
    * ############################################################
    * # WARNING DO NOT CHANGE UNLESS YOU KNOW WHAT YOU ARE DOING #
    * ############################################################
    *
    * @return String MD5 hash of the base ID
    */
  protected def mintDplaId()(implicit data: T): String = DigestUtils.md5Hex(buildProviderBaseId())

  /**
    * Builds the item URI
    *
    * @return URI
    */
  protected def mintDplaItemUri()(implicit data: T): URI = new URI(s"$baseDplaItemUri${mintDplaId()}")
}
