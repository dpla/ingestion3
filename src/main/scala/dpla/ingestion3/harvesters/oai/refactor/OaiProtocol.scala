package dpla.ingestion3.harvesters.oai.refactor

import scala.collection.TraversableOnce

/**
  * TODO:  Would it be easier for OaiProtocol to take the args (endpoint: String,
  * metadataPrefix: Option[String] rather than a whole OaiConfiguration?
  *
  * @param oaiConfiguration
  */

class OaiProtocol(oaiConfiguration: OaiConfiguration) extends OaiMethods with Serializable {

  lazy val endpoint: String = oaiConfiguration.endpoint
  lazy val metadataPrefix: Option[String] = oaiConfiguration.metadataPrefix

  override def listAllRecordPages: TraversableOnce[Either[OaiError, OaiPage]] = {
    val responseBuilder = new OaiMultiPageResponseBuilder(endpoint,
      "ListRecords", metadataPrefix)

    val multiPageResponse = responseBuilder.getResponse
    // TODO: Is there a better way to make a TraversableOnce return type?
    multiPageResponse.toIterator
  }

  override def listAllRecordPagesForSet(setEither: Either[OaiError, OaiSet]):
    TraversableOnce[Either[OaiError, OaiPage]] = {

    val listResponse: List[Either[OaiError, OaiPage]] = setEither match {
      case Left(error) => List(Left(error))
      case Right(set) => {

        val responseBuilder = new OaiMultiPageResponseBuilder(endpoint,
          "ListRecords", metadataPrefix, Some(set.id))

        responseBuilder.getResponse
      }
    }
    // TODO: Is there a better way to make a TraversableOnce return type?
    listResponse.toIterator
  }

  override def listAllSetPages: TraversableOnce[Either[OaiError, OaiPage]] = {
    val responseBuilder = new OaiMultiPageResponseBuilder(endpoint, "ListSets")
    val multiPageResponse = responseBuilder.getResponse
    // TODO: Is there a better way to make a TraversableOnce return type?
    multiPageResponse.toIterator
  }

  override def parsePageIntoRecords(pageEither: Either[OaiError, OaiPage]):
    TraversableOnce[Either[OaiError, OaiRecord]] = {

    val xmlEither = OaiXmlParser.parsePageIntoXml(pageEither)
    val records = OaiXmlParser.parseXmlIntoRecords(xmlEither)
    // TODO: Is there a better way to make a TraversableOnce return type?
    records.toIterator
  }

  override def parsePageIntoSets(pageEither: Either[OaiError, OaiPage]):
    TraversableOnce[Either[OaiError, OaiSet]] = {

    val xmlEither = OaiXmlParser.parsePageIntoXml(pageEither)
    val sets = OaiXmlParser.parseXmlIntoSets(xmlEither)
    // TODO: Is there a better way to make a TraversableOnce return type?
    sets.toIterator
  }
}
