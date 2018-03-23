package dpla.ingestion3.enrichments.normalizations

import dpla.ingestion3.model._

/**
  * Enrichment to remove duplicate values in multi-value fields.
  *
  */
class Deduplication {

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
      hasView = record.hasView.map(enrichEdmWebResource(_)).distinct,
      intermediateProvider = record.intermediateProvider.map(enrichEdmAgent(_)),
      isShownAt = enrichEdmWebResource(record.isShownAt),
      `object` = record.`object`.map(enrichEdmWebResource(_)),
      preview = record.preview.map(enrichEdmWebResource(_)),
      provider = enrichEdmAgent(record.provider)
    )
  }

  def enrichSourceResource(sourceResource: DplaSourceResource): DplaSourceResource =
    sourceResource.copy(
      alternateTitle = sourceResource.alternateTitle.distinct,
      collection = sourceResource.collection.distinct,
      contributor = sourceResource.contributor.map(enrichEdmAgent(_)).distinct,
      creator = sourceResource.creator.map(enrichEdmAgent(_)).distinct,
      date = sourceResource.date.distinct,
      description = sourceResource.description.distinct,
      extent = sourceResource.extent.distinct,
      format = sourceResource.format.distinct,
      genre = sourceResource.genre.map(enrichSkosConcept(_)).distinct,
      identifier = sourceResource.identifier.distinct,
      language = sourceResource.language.map(enrichSkosConcept(_)).distinct,
      place = sourceResource.place.distinct,
      publisher = sourceResource.publisher.map(enrichEdmAgent(_)).distinct,
      relation = sourceResource.relation.distinct,
      replacedBy = sourceResource.replacedBy.distinct,
      replaces = sourceResource.replaces.distinct,
      rights = sourceResource.rights.distinct,
      rightsHolder = sourceResource.rightsHolder.map(enrichEdmAgent(_)).distinct,
      subject = sourceResource.subject.map(enrichSkosConcept(_)).distinct,
      temporal = sourceResource.temporal.distinct,
      title = sourceResource.title.distinct,
      `type` = sourceResource.`type`.distinct
    )

  def enrichEdmAgent(edmAgent: EdmAgent): EdmAgent =
    edmAgent.copy(
      exactMatch = edmAgent.exactMatch.distinct,
      closeMatch = edmAgent.closeMatch.distinct
    )

  def enrichEdmWebResource(edmWebResource: EdmWebResource): EdmWebResource =
    edmWebResource.copy(
      fileFormat = edmWebResource.fileFormat.distinct,
      dcRights = edmWebResource.dcRights.distinct
    )

  def enrichSkosConcept(skosConcept: SkosConcept): SkosConcept =
    skosConcept.copy(
      exactMatch = skosConcept.exactMatch.distinct,
      closeMatch = skosConcept.closeMatch.distinct
    )
}
