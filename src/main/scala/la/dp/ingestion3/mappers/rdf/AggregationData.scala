package la.dp.ingestion3.mappers.rdf

import org.eclipse.rdf4j.model.{IRI, Resource}

case class AggregationData(aggregatedCHO: Resource,
                           isShownAt: IRI,
                           preview: Option[Resource],
                           provider: IRI,
                           originalRecord: Resource,
                           dataProvider: Resource)
