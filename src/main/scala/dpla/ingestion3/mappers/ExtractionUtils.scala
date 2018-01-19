package dpla.ingestion3.mappers

import dpla.ingestion3.model._
import dpla.ingestion3.utils.ProviderRegistry

import scala.util.Try


/**
  *
  */
object Mapper {

  /**
    * Executes the transformation of the provided original record to DPLA MAP OreAggregation
    *
    * @param shortname String Provider's shortname/abbreviation used to load its mapper, parser
    *                  and harvester from Registry
    * @param data String Original record to transform
    * @return
    */
  def build(shortname: String, data: String): Try[OreAggregation] = {

    val reg = ProviderRegistry.lookupRegister(shortname)
      .getOrElse(throw new RuntimeException(s"Failed to load registry for '$shortname'"))

    // Parse the string representation into
    val originalRecord = reg.parser.parse(data)
    val mapper = reg.mapper

    Try {
      OreAggregation(
        dplaUri = mapper.dplaUri(originalRecord),
        dataProvider = mapper.dataProvider(originalRecord),
        originalRecord = data,
        hasView = mapper.hasView(originalRecord),
        intermediateProvider = mapper.intermediateProvider(originalRecord),
        isShownAt = mapper.isShownAt(originalRecord),
        `object` = mapper.`object`(originalRecord), // full size image
        preview = mapper.preview(originalRecord), // thumbnail
        provider = mapper.provider(originalRecord),
        edmRights = mapper.edmRights(originalRecord),
        sidecar = mapper.sidecar(originalRecord),
        sourceResource = DplaSourceResource(
          alternateTitle = mapper.alternateTitle(originalRecord),
          collection = mapper.collection(originalRecord),
          contributor = mapper.contributor(originalRecord),
          creator = mapper.creator(originalRecord),
          date = mapper.date(originalRecord),
          description = mapper.description(originalRecord),
          extent = mapper.extent(originalRecord),
          format = mapper.format(originalRecord),
          genre = mapper.genre(originalRecord),
          identifier = mapper.identifier(originalRecord),
          language = mapper.language(originalRecord),
          place = mapper.place(originalRecord),
          publisher = mapper.publisher(originalRecord),
          relation = mapper.relation(originalRecord),
          replacedBy = mapper.replacedBy(originalRecord),
          replaces = mapper.replaces(originalRecord),
          rights = mapper.rights(originalRecord),
          rightsHolder = mapper.rightsHolder(originalRecord),
          subject = mapper.subject(originalRecord),
          temporal = mapper.temporal(originalRecord),
          title = mapper.title(originalRecord),
          `type`= mapper.`type`(originalRecord)
        )
      )
    }
  }
}

