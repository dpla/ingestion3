package dpla.ingestion3.harvesters.oai.refactor

import scala.collection.TraversableOnce

class OaiProtocol(oaiConfiguration: OaiConfiguration) extends OaiMethods {

  lazy val endpoint = oaiConfiguration.endpoint

  override def listAllRecordPages: TraversableOnce[Either[OaiError, OaiPage]] = {

    val metadataPrefix = oaiConfiguration.metadataPrefix.getOrElse(
      // Fatal exception.
      throw new RuntimeException("metadataPrefix not found")
    )

    val baseParams = Map("endpoint" -> endpoint, "verb" -> "ListRecords")
    val opts = Map("metadataPrefix" -> metadataPrefix)

    val multiPageResponse = OaiMultiPageResponseBuilder.getResponse(baseParams, opts)
    // TODO: This is a convenient but probably not very useful way to make the
    // return type a TraversableOnce until I figure out a better way.
    multiPageResponse.toIterator
  }

  override def listAllRecordPagesForSet(setEither: Either[OaiError, OaiSet]):
    TraversableOnce[Either[OaiError, OaiPage]] = {

    val listResponse: List[Either[OaiError, OaiPage]] = setEither match {
      case Left(error) => List(Left(error))
      case Right(set) => {

        val metadataPrefix = oaiConfiguration.metadataPrefix.getOrElse(
          // Fatal exception.
          throw new RuntimeException("metadataPrefix not found")
        )

        val baseParams = Map("endpoint" -> endpoint, "verb" -> "ListRecords")
        val opts = Map("metadataPrefix" -> metadataPrefix, "set" -> set.id)

        OaiMultiPageResponseBuilder.getResponse(baseParams, opts)
      }
    }
    // TODO: Is there a better way to make a TraversableOnce return type?
    listResponse.toIterator
  }

  override def listAllSetPages: TraversableOnce[Either[OaiError, OaiPage]] = {
    val baseParams = Map("endpoint" -> endpoint, "verb" -> "ListSets")
    val multiPageResponse = OaiMultiPageResponseBuilder.getResponse(baseParams)
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
