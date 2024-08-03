package dpla.ingestion3.harvesters.oai.refactor

import org.apache.logging.log4j.LogManager

class OaiProtocol(oaiConfiguration: OaiConfiguration)
    extends OaiMethods
    with Serializable {

  private val logger = LogManager.getLogger(this.getClass)

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

    logger.info("IN: listAllRecordPagesForSet {}", set)
    new OaiMultiPageResponseBuilder(
      endpoint,
      "ListRecords",
      metadataPrefix,
      Some(set.id)
    ).getResponse.iterator
  }

  override def listAllSetPages(): IterableOnce[OaiPage] = {
    logger.info("IN: listAllSetPages")
    new OaiMultiPageResponseBuilder(endpoint, "ListSets").getResponse.iterator
  }

  override def parsePageIntoRecords(
      page: OaiPage,
      removeDeleted: Boolean
  ): IterableOnce[OaiRecord] = {
    logger.info("IN: parsePageIntoRecords")
    OaiXmlParser
      .parseXmlIntoRecords(OaiXmlParser.parsePageIntoXml(page), removeDeleted)
      .iterator
  }

  override def parsePageIntoSets(
      page: OaiPage
  ): IterableOnce[OaiSet] = {
    logger.info("IN: parsePageIntoSets")
    OaiXmlParser.parseXmlIntoSets(OaiXmlParser.parsePageIntoXml(page)).iterator
  }
}
