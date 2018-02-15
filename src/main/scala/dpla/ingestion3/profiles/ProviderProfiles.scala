package dpla.ingestion3.profiles

import dpla.ingestion3.mappers.providers.PaMapping

class PaProfile extends XmlProfile {
  type Mapping = PaMapping
  override def getMapping = new PaMapping
}
