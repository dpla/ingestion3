package dpla.ingestion3.harvesters.oai

import scala.util.{Failure, Success, Try}

class OaiProtocol(oaiConfiguration: OaiConfiguration,
                  harvestLogger: OaiHarvestLogger = OaiHarvestLogger.Noop)
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
      oaiConfiguration.sleep,
      harvestLogger
    ).getResponse.iterator

  override def listAllRecordPagesForSet(
      setSpec: String
  ): IterableOnce[OaiPage] = {
    new OaiMultiPageResponseBuilder(
      endpoint,
      "ListRecords",
      metadataPrefix,
      Some(setSpec),
      oaiConfiguration.sleep,
      harvestLogger
    ).getResponse.iterator
  }

  override def listAllSetPages(): IterableOnce[OaiPage] = {
    new OaiMultiPageResponseBuilder(endpoint, "ListSets", None, None, oaiConfiguration.sleep, harvestLogger).getResponse.iterator
  }

  override def parsePageIntoRecords(
      page: OaiPage,
      removeDeleted: Boolean
  ): IterableOnce[OaiRecord] = {
    Try {
      OaiXmlParser
        .parseXmlIntoRecords(OaiXmlParser.parsePageIntoXml(page), removeDeleted, page.info)
        .iterator
    } match {
      case Success(records) => records
      case Failure(e: OaiHarvestException) => throw e
      case Failure(e) =>
        val (firstId, lastId) = OaiXmlParser.extractIdentifiers(page.page)
        throw new OaiHarvestException(
          requestInfo = page.info,
          url = endpoint,
          stage = "page_parse",
          firstId = firstId,
          lastId = lastId,
          cause = e
        )
    }
  }

  override def parsePageIntoSets(
      page: OaiPage
  ): IterableOnce[OaiSet] = {
    Try {
      OaiXmlParser.parseXmlIntoSets(OaiXmlParser.parsePageIntoXml(page), page.info).iterator
    } match {
      case Success(sets) => sets
      case Failure(e: OaiHarvestException) => throw e
      case Failure(e) =>
        throw new OaiHarvestException(
          requestInfo = page.info,
          url = endpoint,
          stage = "set_parse",
          cause = e
        )
    }
  }
}
