package la.dp.ingestion3.mappers.pa

import la.dp.ingestion3.mappers.rdf.{AggregationData, ChoData, MappingUtils}
import org.eclipse.rdf4j.model.Model


class PAMapper(document: PADocument) extends MappingUtils {

  def map(): Model = {
    // There must be two or more identifiers provided by PA
    assert(document.identifiers.size >= 2)

    val providerLabel = "PA Digital"
    val paUri = iri("http://dp.la/api/contributor/pa")

    val thumbnail = document.identifiers match {
      case ids if ids.size > 2 => Some(iri(ids.last))
      case _ => None
    }

    val link = iri(document.identifiers(1))
    val paCollection = Seq(document.relations.head)

    registerNamespaces(defaultVocabularies)
    mapItemWebResource(link)
    mapContributingAgent(paUri, providerLabel)

    for (t <- thumbnail)
      mapThumbWebResource(t)

    val aggregatedCHO = mapAggregatedCHO(ChoData(
      dates = mapDates(document.dates.distinct),
      titles = mapStrings(document.titles.distinct),
      identifiers = mapStrings(document.identifiers.distinct),
      rights = mapStrings(document.rights.distinct),
      collections = mapStrings(paCollection),
      contributors = mapStrings(document.contributors.distinct),
      creators = mapStrings(document.creators.distinct),
      publishers = mapStrings(document.publishers.distinct),
      types = mapStrings(document.types.distinct)
    ))

    mapAggregation(AggregationData(
      aggregatedCHO = aggregatedCHO,
      isShownAt = link,
      preview = thumbnail,
      provider = paUri,
      originalRecord = mapOriginalRecord(),
      dataProvider = mapDataProvider(providerLabel)
    ))

    build()
  }
}
