package dpla.ingestion3.harvesters.oai.refactor

import java.net.URL

import dpla.ingestion3.harvesters.UrlBuilder
import dpla.ingestion3.harvesters.oai._
import dpla.ingestion3.utils.HttpUtils
import org.apache.http.client.utils.URIBuilder

import scala.annotation.tailrec
import scala.collection.TraversableOnce
import scala.util.{Failure, Success, Try}

class OaiProtocol(oaiConfiguration: OaiConfiguration) extends OaiMethods with UrlBuilder {

  override def listAllRecordPages:
    TraversableOnce[Either[OaiPage, OaiError]] = {

    val metadataPrefix = oaiConfiguration.metadataPrefix
    val endpoint = oaiConfiguration.endpoint

    val baseParams = Map("endpoint" -> endpoint, "verb" -> "ListRecords")
    val multiPageResponse: List[OaiResponse] = getMultiPageResponse(baseParams)
  }

  override def listAllRecordPagesForSet(set: String): TraversableOnce[Either[OaiRecord, OaiError]] = ???

  override def parsePageIntoRecords(page: String): TraversableOnce[Either[OaiRecord, OaiError]] = ???

  override def listAllSets: TraversableOnce[Either[OaiSet, OaiError]] = ???


  def getMultiPageResponse(baseParams: Map[String, String],
                           opts: Map[String, String] = Map()): List[OaiResponse] = ???
}
