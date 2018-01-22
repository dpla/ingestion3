package dpla.ingestion3.mappers

import dpla.ingestion3.mappers.utils.{JsonMapping, Mapping, XmlMapping}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import dpla.ingestion3.utils.Utils.formatJson
import org.json4s.JValue

import scala.util.Try
import scala.xml.NodeSeq

trait Mapper[T, E] {
  def map(document: T, mapping: Mapping[T]): Try[OreAggregation]
}


// FIXME
class XmlMapper extends Mapper[NodeSeq, XmlMapping] {

  override def map(document: NodeSeq, mapping: Mapping[NodeSeq]): Try[OreAggregation] = {
    Try {
      OreAggregation(
        dplaUri = mapping.dplaUri(document),
        dataProvider = mapping.dataProvider(document),
        edmRights = mapping.edmRights(document),
        hasView = mapping.hasView(document),
        intermediateProvider = mapping.intermediateProvider(document),
        isShownAt = mapping.isShownAt(document),
        `object` = mapping.`object`(document), // full size image
        originalRecord = Utils.formatXml(document),
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
          `type`= mapping.`type`(document)
        )
      )
    }
  }
}

class JsonMapper extends Mapper[JValue, _ <: JsonMapping] {
  override def map(document: JValue, mapping: Mapping[JValue]): Try[OreAggregation] = {
    Try {
      OreAggregation(
        dplaUri = mapping.dplaUri(document),
        dataProvider = mapping.dataProvider(document),
        edmRights = mapping.edmRights(document),
        hasView = mapping.hasView(document),
        intermediateProvider = mapping.intermediateProvider(document),
        isShownAt = mapping.isShownAt(document),
        `object` = mapping.`object`(document), // full size image
        originalRecord = formatJson(document),
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
          `type`= mapping.`type`(document)
        )
      )
    }
  }
}

//
//object Mapper {
//
//  /**
//    * Executes the transformation of the provided original record to DPLA MAP OreAggregation
//    *
//    * @param shortname String Provider's shortname/abbreviation used to load its mapper, parser
//    *                  and harvester from Registry
//    * @param data String Original record to transform
//    * @return
//    */
//  def build(shortname: String, data: String): Try[OreAggregation] = {
//
//    val register = ProviderRegistry.lookupRegister(shortname)
//      .getOrElse(throw new RuntimeException(s"Failed to load registry for '$shortname'"))
//
//    // Parse the string representation into NodeSeq, JValue or other appropriate representation
//    val document = register.parser.parse(data)
//    // Gets the mapper class from the ProviderRegistry register
//    val mapping = register.mapper
//
//    Try {
//      OreAggregation(
//        dplaUri = mapping.dplaUri(document),
//        dataProvider = mapping.dataProvider(document),
//        edmRights = mapping.edmRights(document),
//        hasView = mapping.hasView(document),
//        intermediateProvider = mapping.intermediateProvider(document),
//        isShownAt = mapping.isShownAt(document),
//        `object` = mapping.`object`(document), // full size image
//        originalRecord = data,
//        preview = mapping.preview(document), // thumbnail
//        provider = mapping.provider(document),
//        sidecar = mapping.sidecar(document),
//        sourceResource = DplaSourceResource(
//          alternateTitle = mapping.alternateTitle(document),
//          collection = mapping.collection(document),
//          contributor = mapping.contributor(document),
//          creator = mapping.creator(document),
//          date = mapping.date(document),
//          description = mapping.description(document),
//          extent = mapping.extent(document),
//          format = mapping.format(document),
//          genre = mapping.genre(document),
//          identifier = mapping.identifier(document),
//          language = mapping.language(document),
//          place = mapping.place(document),
//          publisher = mapping.publisher(document),
//          relation = mapping.relation(document),
//          replacedBy = mapping.replacedBy(document),
//          replaces = mapping.replaces(document),
//          rights = mapping.rights(document),
//          rightsHolder = mapping.rightsHolder(document),
//          subject = mapping.subject(document),
//          temporal = mapping.temporal(document),
//          title = mapping.title(document),
//          `type`= mapping.`type`(document)
//        )
//      )
//    }
//  }
//}
//
