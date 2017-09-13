package dpla.ingestion3.model

import dpla.ingestion3.model.DplaMapData.LiteralOrUri
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}

/**
  * Responsible for desugaring a DplaMapModel and converting it to a Spark-native Row-based structure.
  *
  * PLEASE NOTE: The *ordering* of the parameters to each call to the Row() constructor is significant, and
  * cannot be changed without a concurrent effort to rewrite all the data in the master dataset, and a matching
  * edit to ModelConverter which is responsible for creating sugared instances of DplaMapData from the output of this
  * code, and represents the opposite of this transformation.
  */

object RowConverter {

  def toRow(dplaMapData: DplaMapData, sqlSchema: StructType): Row =
    new GenericRowWithSchema(
      Array(dplaSourceResource(dplaMapData.sourceResource),
        edmWebResource(dplaMapData.edmWebResource),
        oreAggregation(dplaMapData.oreAggregation)),
      sqlSchema
    )

  private[model] def oreAggregation(oa: OreAggregation): Row = Row(
    oa.uri.toString,
    edmAgent(oa.dataProvider),
    oa.originalRecord,
    oa.hasView.map(edmWebResource),
    oa.intermediateProvider.map(edmAgent).orNull,
    oa.`object`.map(edmWebResource).orNull,
    oa.preview.map(edmWebResource).orNull,
    edmAgent(oa.provider),
    oa.edmRights.map(_.toString).orNull
  )

  private[model] def dplaSourceResource(sr: DplaSourceResource): Row = Row(
    sr.alternateTitle,
    sr.collection.map(dcmiTypeCollection),
    sr.contributor.map(edmAgent),
    sr.creator.map(edmAgent),
    sr.date.map(edmTimeSpan),
    sr.description,
    sr.extent,
    sr.format,
    sr.genre.map(skosConcept),
    sr.identifier,
    sr.language.map(skosConcept),
    sr.place.map(dplaPlace),
    sr.publisher.map(edmAgent),
    sr.relation.map(literalOrUri),
    sr.replacedBy,
    sr.replaces,
    sr.rights,
    sr.rightsHolder.map(edmAgent),
    sr.subject.map(skosConcept),
    sr.temporal.map(edmTimeSpan),
    sr.title,
    sr.`type`
  )

  private[model]def edmWebResource(wr: EdmWebResource): Row = Row(
    wr.uri.toString,
    wr.fileFormat,
    wr.dcRights,
    wr.edmRights.orNull
  )

  private[model] def edmAgent(ea: EdmAgent): Row = Row(
    ea.uri.map(_.toString).orNull,
    ea.name.orNull,
    ea.providedLabel.orNull,
    ea.note.orNull,
    ea.scheme.map(_.toString).orNull,
    ea.exactMatch.map(_.toString),
    ea.closeMatch.map(_.toString)
  )


  private[model] def literalOrUri(literalOrUri: LiteralOrUri): Row = Row(
    literalOrUri.merge.toString, //both types turn into strings with toString
    literalOrUri.isRight //right is URI, isRight is true when it's a uri
  )

  private[model] def dplaPlace(dplaPlace: DplaPlace): Row = Row(
    dplaPlace.name.orNull,
    dplaPlace.city.orNull,
    dplaPlace.county.orNull,
    dplaPlace.region.orNull,
    dplaPlace.state.orNull,
    dplaPlace.country.orNull,
    dplaPlace.coordinates.orNull
  )

  private[model] def skosConcept(skosConcept: SkosConcept): Row = Row(
    skosConcept.concept.orNull,
    skosConcept.providedLabel.orNull,
    skosConcept.note.orNull,
    skosConcept.scheme.map(_.toString).orNull,
    skosConcept.exactMatch.map(_.toString),
    skosConcept.closeMatch.map(_.toString)
  )

  private[model] def edmTimeSpan(edmTimeSpan: EdmTimeSpan): Row = Row(
    edmTimeSpan.originalSourceDate.orNull,
    edmTimeSpan.prefLabel.orNull,
    edmTimeSpan.begin.orNull,
    edmTimeSpan.end.orNull
  )

  private[model] def dcmiTypeCollection(dcmiTypeCollection: DcmiTypeCollection): Row = Row(
    dcmiTypeCollection.title.orNull,
    dcmiTypeCollection.description.orNull
  )

}
