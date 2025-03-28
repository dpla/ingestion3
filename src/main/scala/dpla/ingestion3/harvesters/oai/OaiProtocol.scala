package dpla.ingestion3.harvesters.oai

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
      setSpec: String
  ): IterableOnce[OaiPage] = {
    new OaiMultiPageResponseBuilder(
      endpoint,
      "ListRecords",
      metadataPrefix,
      Some(setSpec),
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
      .parseXmlIntoRecords(OaiXmlParser.parsePageIntoXml(page), removeDeleted, page.info)
      .iterator
  }

  override def parsePageIntoSets(
      page: OaiPage
  ): IterableOnce[OaiSet] = {
    OaiXmlParser.parseXmlIntoSets(OaiXmlParser.parsePageIntoXml(page), page.info).iterator
  }
}
