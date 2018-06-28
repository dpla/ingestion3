package dpla.ingestion3.mappers

import dpla.ingestion3.mappers.utils._
import dpla.ingestion3.messages.{IngestMessage, MessageCollector}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils._
import org.json4s.JsonAST._

import scala.util.{Failure, Success, Try}
import scala.xml.NodeSeq

trait Mapper[T, +E] {
  def map(document: Document[T], mapping: Mapping[T]): (Option[OreAggregation], Option[String])

  /**
    *
    * @param map
    * @param msgCollector
    * @return Tuple (Option[OreAggregation], Option[String])
    */
  def getReturnValue(map: Try[OreAggregation])
                    (implicit msgCollector: MessageCollector[IngestMessage]): (Some[OreAggregation], Option[String]) =
    map match {
      case Success(s) => (Some(s.copy(messages = msgCollector.getAll())), None)
      case Failure(f) => (Some(emptyOreAggregation.copy(messages = msgCollector.getAll())), Some(f.getMessage))
    }
}

class XmlMapper extends Mapper[NodeSeq, XmlMapping] {
  override def map(document: Document[NodeSeq], mapping: Mapping[NodeSeq]): (Option[OreAggregation], Option[String]) = {
    implicit val msgCollector: MessageCollector[IngestMessage] = new MessageCollector[IngestMessage]

    val mapResult = Try(
      OreAggregation(
        dplaUri = mapping.dplaUri(document),
        dataProvider = mapping.dataProvider(document),
        edmRights = mapping.edmRights(document),
        hasView = mapping.hasView(document),
        intermediateProvider = mapping.intermediateProvider(document),
        isShownAt = mapping.isShownAt(document),
        `object` = mapping.`object`(document), // full size image
        originalRecord = formatXml(document.get),
        preview = mapping.preview(document), // thumbnail
        provider = mapping.provider(document),
        sidecar = mapping.sidecar(document),
        sourceResource = DplaSourceResource(
          alternateTitle = mapping.alternateTitle(document),
          collection = mapping.collection(document),
          contributor = mapping.contributor(document),
          creator = mapping.creator(document),
          date = mapping.date(document),
          description = mapping.description(document),
          extent = mapping.extent(document),
          format = mapping.format(document),
          genre = mapping.genre(document),
          identifier = mapping.identifier(document),
          language = mapping.language(document),
          place = mapping.place(document),
          publisher = mapping.publisher(document),
          relation = mapping.relation(document),
          replacedBy = mapping.replacedBy(document),
          replaces = mapping.replaces(document),
          rights = mapping.rights(document),
          rightsHolder = mapping.rightsHolder(document),
          subject = mapping.subject(document),
          temporal = mapping.temporal(document),
          title = mapping.title(document),
          `type` = mapping.`type`(document)
        )
      )
    )
    getReturnValue(mapResult)
  }
}

class JsonMapper extends Mapper[JValue, JsonMapping] {
  override def map(document: Document[JValue], mapping: Mapping[JValue]): (Option[OreAggregation], Option[String]) = {

    implicit val msgCollector: MessageCollector[IngestMessage] = new MessageCollector[IngestMessage]

    val mapResult = Try (
      OreAggregation(
        dplaUri = mapping.dplaUri(document),
        dataProvider = mapping.dataProvider(document),
        edmRights = mapping.edmRights(document),
        hasView = mapping.hasView(document),
        intermediateProvider = mapping.intermediateProvider(document),
        isShownAt = mapping.isShownAt(document),
        `object` = mapping.`object`(document), // full size image
        originalRecord = formatJson(document.get),
        preview = mapping.preview(document), // thumbnail
        provider = mapping.provider(document),
        sidecar = mapping.sidecar(document),
        sourceResource = DplaSourceResource(
          alternateTitle = mapping.alternateTitle(document),
          collection = mapping.collection(document),
          contributor = mapping.contributor(document),
          creator = mapping.creator(document),
          date = mapping.date(document),
          description = mapping.description(document),
          extent = mapping.extent(document),
          format = mapping.format(document),
          genre = mapping.genre(document),
          identifier = mapping.identifier(document),
          language = mapping.language(document),
          place = mapping.place(document),
          publisher = mapping.publisher(document),
          relation = mapping.relation(document),
          replacedBy = mapping.replacedBy(document),
          replaces = mapping.replaces(document),
          rights = mapping.rights(document),
          rightsHolder = mapping.rightsHolder(document),
          subject = mapping.subject(document),
          temporal = mapping.temporal(document),
          title = mapping.title(document),
          `type` = mapping.`type`(document)
        ),
        messages = msgCollector.getAll()
      )
    )
    getReturnValue(mapResult)
  }
}