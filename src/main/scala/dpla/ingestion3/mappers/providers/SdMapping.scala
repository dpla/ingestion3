package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils._

class SdMapping extends MdlMapping with JsonMapping with JsonExtractor {

  // ID minting functions
  override def useProviderName: Boolean = true

  override def getProviderName: Option[String] = Some("sd")
}
