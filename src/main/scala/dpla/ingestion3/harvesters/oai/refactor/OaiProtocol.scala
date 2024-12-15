package dpla.ingestion3.harvesters.oai.refactor

import org.apache.logging.log4j.LogManager

class OaiProtocol(oaiConfiguration: OaiConfiguration)
    extends OaiMethods
    with Serializable {

  lazy val endpoint: String = oaiConfiguration.endpoint
  lazy val metadataPrefix: Option[String] = oaiConfiguration.metadataPrefix

  override def listAllRecordPages(): IterableOnce[OaiPage] =
    new OaiMultiPageResponseBuilder(
      endpoint,
      "ListRecords",
      metadataPrefix,
      None,
      oaiConfiguration.sleep
    ).getResponse.iterator

  override def listAllRecordPagesForSet(
      set: OaiSet
  ): IterableOnce[OaiPage] = {
    new OaiMultiPageResponseBuilder(
      endpoint,
      "ListRecords",
      metadataPrefix,
      Some(set.id),
      oaiConfiguration.sleep
    ).getResponse.iterator
  }

  override def listAllSetPages(): IterableOnce[OaiPage] = {
    new OaiMultiPageResponseBuilder(endpoint, "ListSets", None, None, oaiConfiguration.sleep).getResponse.iterator
  }

  override def parsePageIntoRecords(
      page: OaiPage,
      removeDeleted: Boolean
  ): IterableOnce[OaiRecord] = {
    OaiXmlParser
      .parseXmlIntoRecords(OaiXmlParser.parsePageIntoXml(page), removeDeleted)
      .iterator
  }

  override def parsePageIntoSets(
      page: OaiPage
  ): IterableOnce[OaiSet] = {
    OaiXmlParser.parseXmlIntoSets(OaiXmlParser.parsePageIntoXml(page)).iterator
  }
}
