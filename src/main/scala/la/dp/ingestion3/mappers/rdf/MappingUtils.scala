package la.dp.ingestion3.mappers.rdf

import org.eclipse.rdf4j.model.util.ModelBuilder
import org.eclipse.rdf4j.model.{IRI, Literal, Model, Resource}

trait MappingUtils extends DefaultVocabularies with RdfValueUtils {

  private val builder = new ModelBuilder()

  def build(): Model = builder.build()

  def mapAggregatedCHO(choData: ChoData): Resource = {

    //this function assumes the builder.subject() is set
    def addAll(predicate: IRI, values: Seq[Object]) =
      for (value <- values) {
        builder.add(predicate, value)
      }

    val aggregatedCHO = bnode()

    builder
      .subject(aggregatedCHO)
      .add(rdf.`type`, dpla.sourceResource)

    addAll(dc.date, choData.dates)
    addAll(dcTerms.title, choData.titles)
    addAll(dc.identifier, choData.identifiers)
    addAll(dc.rights, choData.rights)
    addAll(dcTerms.contributor, choData.contributors)
    addAll(dcTerms.creator, choData.creators)
    addAll(dcTerms.isPartOf, choData.collections)
    addAll(dcTerms.publisher, choData.publishers)
    addAll(dcTerms.`type`, choData.types)

    aggregatedCHO
  }

  def mapAggregation(aggregation: AggregationData): Resource = {
    val aggregationBNode = bnode()
    builder.subject(aggregationBNode)
      .add(rdf.`type`, ore.Aggregation)
      .add(edm.aggregatedCHO, aggregation.aggregatedCHO)
      .add(edm.isShownAt, aggregation.isShownAt)
      .add(edm.preview, aggregation.preview)
      .add(edm.provider, aggregation.provider)
      .add(dpla.originalRecord, aggregation.originalRecord)
      .add(edm.dataProvider, aggregation.dataProvider)
    aggregationBNode
  }

  def mapOriginalRecord(): Resource = {
    val originalRecord = bnode()
    builder
      .subject(originalRecord)
      .add(rdf.`type`, edm.WebResource)
    originalRecord
  }

  def mapDataProvider(dataProvider: String): Resource = {
    val dataProviderBnode = bnode()
    builder.subject(dataProviderBnode)
      .add(rdf.`type`, edm.Agent)
      .add(dpla.providedLabel, dataProvider)
    dataProviderBnode
  }

  def mapContributors(contributors: Seq[String]): Seq[Resource] =
    for (contributor <- contributors) yield {
      val contributorBnode = bnode()
      builder.subject(contributorBnode)
        .add(rdf.`type`, edm.Agent)
        .add(dpla.providedLabel, contributor)
      contributorBnode
    }

  def mapCreators(creators: Seq[String]): Seq[Resource] =
    for (creator <- creators) yield {
      val root = bnode()
      builder.subject(root)
        .add(rdf.`type`, edm.Agent)
        .add(dpla.providedLabel, creator)
      root
    }

  def mapCollections(collectionNames: Seq[String]): Seq[Resource] =
    for (collection <- collectionNames) yield {
      val collectionBnode = bnode()
      builder.subject(collectionBnode)
        .add(rdf.`type`, dcmiType.Collection)
        .add(dcTerms.title, collection)
      collectionBnode
    }

  def mapDates(dates: Seq[String]): Seq[Resource] =
    for (date <- dates) yield {
      val dateBnode = bnode()
      builder.subject(dateBnode)
        .add(rdf.`type`, edm.TimeSpan)
        .add(dpla.providedLabel, date)
      dateBnode
    }

  def mapItemWebResource(itemUrl: IRI): Unit = {
    builder.subject(itemUrl)
      .add(rdf.`type`, edm.WebResource)
  }

  def mapContributingAgent(uri: IRI, label: String): Unit = {
    builder.subject(uri)
      .add(rdf.`type`, edm.Agent)
      .add(skos.prefLabel, label)
  }

  def mapThumbWebResource(thumbnail: IRI): Unit = {
    builder.subject(thumbnail)
      .add(rdf.`type`, edm.WebResource)
  }

  def registerNamespaces(vocabularies: Seq[Vocabulary]): Unit = {
    for (vocabulary <- vocabularies)
      builder.setNamespace(vocabulary.ns)
  }

  def mapStrings(values: Seq[String]): Seq[Literal] =
    for (value <- values) yield literal(value)
}
