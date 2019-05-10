package dpla.ingestion3.enrichments.normalizations

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.model.DplaMapData.LiteralOrUri
import dpla.ingestion3.model._

/**
  * Universal String enrichments.
  *
  */
class StringNormalizations {

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
      edmRights = record.edmRights.map(enrichEdmRights(_)),
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
        .stripBrackets
        .cleanupLeadingPunctuation
        .cleanupEndingPunctuation),
      `type` = sourceResource.`type`.map(_.stripHTML.reduceWhitespace)
    )

  def enrichEdmAgent(edmAgent: EdmAgent): EdmAgent =
    edmAgent.copy(
      name = edmAgent.name.map(
        _.stripHTML
          .reduceWhitespace
          .stripBrackets
          .stripEndingPeriod
          .cleanupLeadingPunctuation
          .cleanupEndingPunctuation)
    )

  def enrichEdmRights(edmRights: URI): URI = {
    val uri = new java.net.URI(edmRights.toString) // value already validated as URI in mapping
    // normalize uri path
    val path = if (uri.getPath.startsWith("/page/")) {
      uri.getPath.replaceFirst("page", "vocab") // rightstatements.org cleanup
    } else
      uri.getPath

    URI(s"https://${uri.getHost}$path") // normalize to https and drop parameters
  }

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
      originalSourceDate = edmTimeSpan.originalSourceDate.map(_.stripHTML.reduceWhitespace),
      prefLabel = edmTimeSpan.prefLabel.map(_.stripHTML.reduceWhitespace.stripDblQuotes),
      begin = edmTimeSpan.begin.map(_.stripHTML.reduceWhitespace.stripDblQuotes),
      end = edmTimeSpan.end.map(_.stripHTML.reduceWhitespace.stripDblQuotes)
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
