package dpla.ingestion3.enrichments.normalizations

import StringUtils._
import dpla.ingestion3.model.DplaMapData.LiteralOrUri
import dpla.ingestion3.model._

/**
  * Universal String enrichments.
  *
  */
class StringEnrichments {

  /**
    * Main entry point.
    *
    * @param record OreAggregation
    * @return enriched OreAggregation
    */
  def enrich(record: OreAggregation): OreAggregation = {
    record.copy(
      sourceResource = enrichSourceResource(record.sourceResource),
      dataProvider = enrichEdmAgent(record.dataProvider),
      hasView = record.hasView.map(enrichEdmWebResource(_)),
      intermediateProvider = record.intermediateProvider.map(enrichEdmAgent(_)),
      isShownAt = enrichEdmWebResource(record.isShownAt),
      `object` = record.`object`.map(enrichEdmWebResource(_)),
      preview = record.preview.map(enrichEdmWebResource(_)),
      provider = enrichEdmAgent(record.provider)
    )
  }

  def enrichSourceResource(sourceResource: DplaSourceResource): DplaSourceResource =
    sourceResource.copy(
      alternateTitle = sourceResource.alternateTitle.map(_.stripHTML.reduceWhitespace),
      collection = sourceResource.collection.map(enrichDcmiTypeCollection(_)),
      contributor = sourceResource.contributor.map(enrichEdmAgent(_)),
      creator = sourceResource.creator.map(enrichEdmAgent(_)),
      date = sourceResource.date.map(enrichEdmTimeSpan(_)),
      description = sourceResource.description.map(_.stripHTML.reduceWhitespace),
      extent = sourceResource.extent.map(_.stripHTML.reduceWhitespace),
      format = sourceResource.format.map(_.stripHTML
        .reduceWhitespace
        .capitalizeFirstChar),
      genre = sourceResource.genre.map(enrichSkosConcept(_)),
      identifier = sourceResource.identifier.map(_.stripHTML.reduceWhitespace),
      language = sourceResource.language.map(enrichSkosConcept(_)),
      place = sourceResource.place.map(enrichDplaPlace(_)),
      publisher = sourceResource.publisher.map(enrichEdmAgent(_)),
      relation = sourceResource.relation.map(enrichRelation(_)),
      replacedBy = sourceResource.replacedBy.map(_.stripHTML.reduceWhitespace),
      replaces = sourceResource.replaces.map(_.stripHTML.reduceWhitespace),
      rights = sourceResource.rights.map(_.stripHTML.reduceWhitespace),
      rightsHolder = sourceResource.rightsHolder.map(enrichEdmAgent(_)),
      subject = sourceResource.subject.map(enrichSkosConcept(_)),
      temporal = sourceResource.temporal.map(enrichEdmTimeSpan(_)),
      title = sourceResource.title.map(_.stripHTML
        .reduceWhitespace
        .cleanupLeadingPunctuation
        .cleanupEndingPunctuation
        .stripBrackets),
      `type` = sourceResource.`type`.map(_.stripHTML.reduceWhitespace)
    )

  def enrichEdmAgent(edmAgent: EdmAgent): EdmAgent =
    edmAgent.copy(
      name = edmAgent.name.map(
        _.stripHTML
          .reduceWhitespace
          .cleanupLeadingPunctuation
          .cleanupEndingPunctuation)
    )

  def enrichEdmWebResource(edmWebResource: EdmWebResource): EdmWebResource =
    edmWebResource.copy(
      fileFormat = edmWebResource.fileFormat.map(_.stripHTML.reduceWhitespace),
      dcRights = edmWebResource.dcRights.map(_.stripHTML.reduceWhitespace),
      edmRights = edmWebResource.edmRights.map(_.stripHTML.reduceWhitespace)
    )

  def enrichSkosConcept(skosConcept: SkosConcept): SkosConcept =
    skosConcept.copy(
      concept = skosConcept.concept.map(
        _.stripHTML
          .reduceWhitespace
          .cleanupLeadingPunctuation
          .cleanupEndingPunctuation
          .stripBrackets
          .stripEndingPeriod
          .capitalizeFirstChar),
      providedLabel = skosConcept.providedLabel.map(
        _.stripHTML
          .reduceWhitespace
          .cleanupLeadingPunctuation
          .cleanupEndingPunctuation
          .stripBrackets
          .stripEndingPeriod
          .capitalizeFirstChar)
    )

  def enrichEdmTimeSpan(edmTimeSpan: EdmTimeSpan): EdmTimeSpan =
    edmTimeSpan.copy(
      prefLabel = edmTimeSpan.prefLabel.map(_.stripHTML.reduceWhitespace),
      begin = edmTimeSpan.begin.map(_.stripHTML.reduceWhitespace),
      end = edmTimeSpan.end.map(_.stripHTML.reduceWhitespace)
    )

  def enrichDplaPlace(dplaPlace: DplaPlace): DplaPlace =
    dplaPlace.copy(
      name = dplaPlace.name.map(_.stripHTML.reduceWhitespace),
      city = dplaPlace.city.map(_.stripHTML.reduceWhitespace),
      county = dplaPlace.county.map(_.stripHTML.reduceWhitespace),
      state = dplaPlace.state.map(_.stripHTML.reduceWhitespace),
      country = dplaPlace.country.map(_.stripHTML.reduceWhitespace),
      region = dplaPlace.region.map(_.stripHTML.reduceWhitespace),
      coordinates = dplaPlace.coordinates.map(_.stripHTML.reduceWhitespace)
    )

  def enrichDcmiTypeCollection(collection: DcmiTypeCollection): DcmiTypeCollection =
    collection.copy(
      title = collection.title.map(_.stripHTML.reduceWhitespace),
      description = collection.description.map(_.stripHTML.reduceWhitespace)
    )

  def enrichRelation(relation: LiteralOrUri): LiteralOrUri = {
    if (relation.isInstanceOf[String])
      relation.toString.stripHTML.reduceWhitespace.asInstanceOf[LiteralOrUri]
    else
      relation
  }
}
