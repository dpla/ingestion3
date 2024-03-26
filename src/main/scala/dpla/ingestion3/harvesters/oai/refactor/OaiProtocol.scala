package dpla.ingestion3.harvesters.oai.refactor


/** TODO: Would it be easier for OaiProtocol to take the args (endpoint: String,
  * metadataPrefix: Option[String] rather than a whole OaiConfiguration?
  *
  * @param oaiConfiguration
  */

class OaiProtocol(oaiConfiguration: OaiConfiguration)
    extends OaiMethods
    with Serializable {

  lazy val endpoint: String = oaiConfiguration.endpoint
  lazy val metadataPrefix: Option[String] = oaiConfiguration.metadataPrefix

  override def listAllRecordPages(): IterableOnce[Either[OaiError, OaiPage]] = {
    val responseBuilder =
      new OaiMultiPageResponseBuilder(endpoint, "ListRecords", metadataPrefix)

    val multiPageResponse = responseBuilder.getResponse
    // TODO: Is there a better way to make a TraversableOnce return type?
    multiPageResponse.iterator
  }

  override def listAllRecordPagesForSet(
      setEither: Either[OaiError, OaiSet]
  ): IterableOnce[Either[OaiError, OaiPage]] = {

    val listResponse = setEither match {
      case Left(error) => List(Left(error))
      case Right(set) => {

        val responseBuilder = new OaiMultiPageResponseBuilder(
          endpoint,
          "ListRecords",
          metadataPrefix,
          Some(set.id)
        )

        responseBuilder.getResponse
      }
    }
    listResponse.iterator
  }

  override def listAllSetPages(): IterableOnce[Either[OaiError, OaiPage]] = {
    val responseBuilder = new OaiMultiPageResponseBuilder(endpoint, "ListSets")
    val multiPageResponse = responseBuilder.getResponse
    multiPageResponse.iterator
  }

  override def parsePageIntoRecords(
      pageEither: Either[OaiError, OaiPage],
      removeDeleted: Boolean
  ): IterableOnce[Either[OaiError, OaiRecord]] = {

    val xmlEither = OaiXmlParser.parsePageIntoXml(pageEither)
    val records = OaiXmlParser.parseXmlIntoRecords(xmlEither, removeDeleted)
    records.iterator
  }

  override def parsePageIntoSets(
      pageEither: Either[OaiError, OaiPage]
  ): IterableOnce[Either[OaiError, OaiSet]] = {

    val xmlEither = OaiXmlParser.parsePageIntoXml(pageEither)
    val sets = OaiXmlParser.parseXmlIntoSets(xmlEither)
    sets.iterator
  }
}
