package dpla.ingestion3.model

import dpla.ingestion3.messages.IngestMessage
import dpla.ingestion3.model.DplaMapData.LiteralOrUri
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import org.json4s.jackson.JsonMethods._

/**
  * Responsible for desugaring a DplaMapModel and converting it to a Spark-native Row-based structure.
  *
  * PLEASE NOTE: The *ordering* of the parameters to each call to the Row() constructor is significant, and
  * cannot be changed without a concurrent effort to rewrite all the data in the master dataset, and a matching
  * edit to ModelConverter which is responsible for creating sugared instances of DplaMapData from the output of this
  * code, and represents the opposite of this transformation.
  */

object RowConverter {

  def toRow(oreAggregation: OreAggregation, sqlSchema: StructType): Row = {

    new GenericRowWithSchema(
      Array(
        oreAggregation.dplaUri.toString, //0
        fromSourceResource(oreAggregation.sourceResource), //1
        fromEdmAgent(oreAggregation.dataProvider), //2
        oreAggregation.originalRecord, //3
        oreAggregation.hasView.map(fromEdmWebResource), //4
        oreAggregation.intermediateProvider.map(fromEdmAgent).orNull, //5
        fromEdmWebResource(oreAggregation.isShownAt), //6
        oreAggregation.`object`.map(fromEdmWebResource).orNull, //7
        oreAggregation.preview.map(fromEdmWebResource).orNull, //8
        fromEdmAgent(oreAggregation.provider), //9
        oreAggregation.edmRights.map(_.toString).orNull, //10
        compact(render(oreAggregation.sidecar)), //11
        oreAggregation.messages.map(fromIngestMessage) //12
      ),
      sqlSchema
    )
  }

  private[model] def fromIngestMessage(im: IngestMessage): Row = Row(
    im.message, // 0
    im.level, // 1
    im.id, // 2
    im.field, // 3
    im.value, // 4
    im.enrichedValue // 5
  )

  private[model] def fromEdmWebResource(wr: EdmWebResource): Row = Row(
    wr.uri.toString, //0
    wr.fileFormat, //1
    wr.dcRights, //2
    wr.edmRights.orNull, //3
    wr.isReferencedBy.toString //4
  )

  private[model] def fromSourceResource(sr: DplaSourceResource): Row = Row(
    sr.alternateTitle, //0
    sr.collection.map(fromDcmiTypeCollection), //1
    sr.contributor.map(fromEdmAgent), //2
    sr.creator.map(fromEdmAgent), //3
    sr.date.map(fromEdmTimeSpan), //4
    sr.description, //5
    sr.extent, //6
    sr.format, //7
    sr.genre.map(fromSkosConcept), //8
    sr.identifier, //9
    sr.language.map(fromSkosConcept), //10
    sr.place.map(fromDplaPlace), //11
    sr.publisher.map(fromEdmAgent), //12
    sr.relation.map(fromLiteralOrUri), //13
    sr.replacedBy, //14
    sr.replaces, //15
    sr.rights, //16
    sr.rightsHolder.map(fromEdmAgent), //17
    sr.subject.map(fromSkosConcept), //18
    sr.temporal.map(fromEdmTimeSpan), //19
    sr.title, //20
    sr.`type` //21
  )

  private[model] def fromEdmAgent(ea: EdmAgent): Row = Row(
    ea.uri.map(_.toString).orNull, //0
    ea.name.orNull, //1
    ea.providedLabel.orNull, //2
    ea.note.orNull, //3
    ea.scheme.map(_.toString).orNull, //4
    ea.exactMatch.map(_.toString), //5
    ea.closeMatch.map(_.toString) //6
  )

  private[model] def fromLiteralOrUri(literalOrUri: LiteralOrUri): Row = Row(
    literalOrUri.merge.toString, //both types turn into strings with toString
    literalOrUri.isRight //right is URI, isRight is true when it's a uri
  )

  private[model] def fromDplaPlace(dplaPlace: DplaPlace): Row = Row(
    dplaPlace.name.orNull, //0
    dplaPlace.city.orNull, //1
    dplaPlace.county.orNull, //2
    dplaPlace.region.orNull, //3
    dplaPlace.state.orNull, //4
    dplaPlace.country.orNull, //5
    dplaPlace.coordinates.orNull //6
  )

  private[model] def fromSkosConcept(skosConcept: SkosConcept): Row = Row(
    skosConcept.concept.orNull, //0
    skosConcept.providedLabel.orNull, //1
    skosConcept.note.orNull, //2
    skosConcept.scheme.map(_.toString).orNull, //3
    skosConcept.exactMatch.map(_.toString), //4
    skosConcept.closeMatch.map(_.toString) //5
  )

  private[model] def fromEdmTimeSpan(edmTimeSpan: EdmTimeSpan): Row = Row(
    edmTimeSpan.originalSourceDate.orNull, //0
    edmTimeSpan.prefLabel.orNull, //1
    edmTimeSpan.begin.orNull, //2
    edmTimeSpan.end.orNull //3
  )

  private[model] def fromDcmiTypeCollection(dcmiTypeCollection: DcmiTypeCollection): Row = Row(
    dcmiTypeCollection.title.orNull, //0
    dcmiTypeCollection.description.orNull //1
  )

}
