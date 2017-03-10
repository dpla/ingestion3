package dpla.ingestion3.mappers.rdf

import org.eclipse.rdf4j.model.util.ModelBuilder
import org.eclipse.rdf4j.model.{IRI, Literal, Model, Resource}

/**
  * A mixin that contains common functions that create the boilerplate of a DPLA MAP RDF document.
  *
  * The assumption is that a mapper will mixin this class.
  *
  * The methods in this trait handle the concern of generating the boilerplate of RDF assertions. The assumption is that
  * the values come "from elsewhere" and there shouldn't be provider-specific code in this class.
  */

trait MappingUtils extends DefaultVocabularies with RdfValueUtils {

  /**
    * Mutable RDF4J model for building graphs using a non-N3-ish syntax.
    *
    * @todo May need to create an accessor for this to allow manipulation
    *       outside of this trait.
    */
  private var builder = new ModelBuilder()

  /**
    * Finalizes the graph and returns it for later serialization
    *
    * @return RD4J Model object
    */
  def build(): Model = builder.build()

  /**
    * Clears out the assertions in this graph. Useful for tests.
    */
  def reset(): Unit = builder = new ModelBuilder()

  /**
    * Builds and returns the "edm:aggregatedCHO bnode for the document
    *
    * @param choData A case class that has the raw values to put
    *                in the assertions in the aggregatedCHO section
    *
    * @return a RDF4J Resource that contains the assertions about the aggregatedCHO.
    */

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

  /**
    * Builds and returns the aggregation ore:Aggregation bnode containing the
    * assertions about the bnode. Relies on having built the aggregatedCHO bnode.
    *
    * @param aggregation A case class that contains the information to build
    *                    the assertions below this bnode
    * @return a RDF4J Resource that contains the assertions about the ore:Aggregation.
    */
  def mapAggregation(aggregation: AggregationData): Resource = {
    val aggregationBNode = bnode()
    builder.subject(aggregationBNode)
      .add(rdf.`type`, ore.Aggregation)
      .add(edm.aggregatedCHO, aggregation.aggregatedCHO)
      .add(edm.isShownAt, aggregation.isShownAt)
      .add(edm.provider, aggregation.provider)
      .add(dpla.originalRecord, aggregation.originalRecord)
      .add(edm.dataProvider, aggregation.dataProvider)

    if (aggregation.preview.isDefined)
      builder.add(edm.preview, aggregation.preview.get)

    aggregationBNode
  }

  /**
    * Builds the bnode containing a single assertion that the original record is an
    * edm:WebResource.
    *
    * @return The bnode as described.
    */
  def mapOriginalRecord(): Resource = {
    val originalRecord = bnode()
    builder
      .subject(originalRecord)
      .add(rdf.`type`, edm.WebResource)
    originalRecord
  }

  /**
    * Builds the bnode containing two assertions that say that the provider is an edm:Agent,
    * and that the dpla:providedLabel is the provider's name.
    *
    * @param dataProvider
    * @return The bnode as described.
    */
  def mapDataProvider(dataProvider: String): Resource = {
    val dataProviderBnode = bnode()
    builder.subject(dataProviderBnode)
      .add(rdf.`type`, edm.Agent)
      .add(dpla.providedLabel, dataProvider)
    dataProviderBnode
  }

  //the following 4 methods maybe should be enrichments.

  /**
    * Takes a sequence of contributor names and builds a sequence containing bnodes about those
    * contributors.
    *
    * @param contributors Names of contributors
    * @return bnodes about contributors.
    */
  def mapContributors(contributors: Seq[String]): Seq[Resource] =
    for (contributor <- contributors) yield {
      val contributorBnode = bnode()
      builder.subject(contributorBnode)
        .add(rdf.`type`, edm.Agent)
        .add(dpla.providedLabel, contributor)
      contributorBnode
    }

  /**
    * Takes a sequence of creators names and builds a sequence containing bnodes about those
    * creators.
    *
    * @param creators Names of creators
    * @return bnodes about creators.
    */
  def mapCreators(creators: Seq[String]): Seq[Resource] =
    for (creator <- creators) yield {
      val root = bnode()
      builder.subject(root)
        .add(rdf.`type`, edm.Agent)
        .add(dpla.providedLabel, creator)
      root
    }

  /**
    * Takes a sequence of collection names and builds a sequence containing bnodes about those
    * collections.
    *
    * @param collections Names of collections
    * @return bnodes about collections.
    */
  def mapCollections(collections: Seq[String]): Seq[Resource] =
    for (collection <- collections) yield {
      val collectionBnode = bnode()
      builder.subject(collectionBnode)
        .add(rdf.`type`, dcmiType.Collection)
        .add(dcTerms.title, collection)
      collectionBnode
    }

  /**
    * Takes a sequence of dates and builds a sequence containing bnodes about those
    * dates.
    *
    * @param dates Names of dates
    * @return bnodes about dates.
    */
  def mapDates(dates: Seq[String]): Seq[Resource] =
    for (date <- dates) yield {
      val dateBnode = bnode()
      builder.subject(dateBnode)
        .add(rdf.`type`, edm.TimeSpan)
        .add(dpla.providedLabel, date)
      dateBnode
    }

  /**
    * Creates an assertion saying the item url is an edm:WebResource.
    *
    * @param itemUrl The url of the item's page on the provider's site.
    */
  def mapItemWebResource(itemUrl: IRI): Unit = {
    builder.subject(itemUrl)
      .add(rdf.`type`, edm.WebResource)
  }

  /**
    * Creates an assertion saying the contributing agent is an edm:Agent
    * and has a skos:prefLabel of their name.
    *
    * @param uri The uri representing the contributing agent.
    * @param label The plain-text name of the contributing agent.
    */
  def mapContributingAgent(uri: IRI, label: String): Unit = {
    builder.subject(uri)
      .add(rdf.`type`, edm.Agent)
      .add(skos.prefLabel, label)
  }

  /**
    * Creates an assertion that the thumbnail uri is an edm:WebResource.
    *
    * @param thumbnail The url of the thumbnail.
    */
  def mapThumbWebResource(thumbnail: IRI): Unit = {
    builder.subject(thumbnail)
      .add(rdf.`type`, edm.WebResource)
  }

  /**
    * Registers a sequence of Vocabularies as namespaces in the document with the
    * associated prefixes as shortcuts.
    *
    * @param vocabularies The sequence of Vocabulary subclasses.
    */
  def registerNamespaces(vocabularies: Seq[Vocabulary]): Unit = {
    for (vocabulary <- vocabularies)
      builder.setNamespace(vocabulary.ns)
  }

  /**
    * A convenience utility function that converts a sequence of strings into
    * a sequence of RDF4J Literals.
    *
    * @param values The strings to convert to literals.
    * @return The Literal instances. Literally.
    */

  //TODO this should probably be moved to RdfValueUtils, and more methods like it created.
  def mapStrings(values: Seq[String]): Seq[Literal] =
    for (value <- values) yield literal(value)
}
