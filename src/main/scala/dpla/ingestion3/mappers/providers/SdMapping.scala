package dpla.ingestion3.mappers.providers

import java.net.URI

import dpla.ingestion3.mappers.utils._
import dpla.ingestion3.model._
import org.json4s._


// FIXME Why is the implicit conversion not working for JValue when it is for NodeSeq?
class SdMapping extends MdlMapping with JsonMapping with JsonExtractor with IdMinter[JValue] {

  // ID minting functions
  override def useProviderName: Boolean = true

  override def getProviderName: String = "sd"

  override def agent = EdmAgent(
    name = Some("Minnesota Digital Library"),
    uri = Some(new URI("http://dp.la/api/contributor/mdl"))
  )
}
