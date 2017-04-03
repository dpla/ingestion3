package dpla.ingestion3.mappers.rdf


import org.eclipse.rdf4j.model.util.Models._
import org.eclipse.rdf4j.model.{BNode, IRI, Value}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class MappingUtilsTest extends FlatSpec with BeforeAndAfter with MappingUtils {

  before {
    reset()
  }

  "A MappingUtils" should "return an empty Model by default" in {
    val model = build()
    assert(model.size === 0)
  }

  it should "clean out when reset is called" in {
    mapOriginalRecord()
    reset()
    val model = build()
    assert(model.size === 0)
  }

  it should "return a bnode when creating the aggregatedCHO" in {
    val aggregatedCHO = mapAggregatedCHO(ChoData())
    assert(aggregatedCHO.isInstanceOf[BNode])
  }

  it should "create an aggregatedCHO that's a dpla:sourceResource" in {
    testCHOMapping(ChoData(), rdf.`type`, Seq(dpla.SourceResource))
  }

  it should "map dates" in {
    val dates = Seq(bnode("1"), bnode("2"), bnode("3"))
    val choData = ChoData(dates = dates)
    testCHOMapping(choData, dc.date, dates)
  }

  it should "map titles" in {
    val titles = Seq(literal("fee"), literal("fie"), literal("fo"), literal("fun"))
    val choData = ChoData(titles = titles)
    testCHOMapping(choData, dcTerms.title, titles)
  }

  it should "map identifiers" in {
    val identifiers = Seq(literal("http://example.com"), literal("http://zombo.com"))
    val choData = ChoData(identifiers = identifiers)
    testCHOMapping(choData, dc.identifier, identifiers)
  }

  it should "map rights" in {
    val rights = Seq(literal("whatev"))
    val choData = ChoData(rights = rights)
    testCHOMapping(choData, dc.rights, rights)
  }

  it should "map contributors" in {
    val contributors = Seq(literal("bob"), literal("mary"))
    val choData = ChoData(contributors = contributors)
    testCHOMapping(choData, dcTerms.contributor, contributors)
  }

  it should "map creators" in {
    val creators = Seq(literal("bob"), literal("mary"))
    val choData = ChoData(creators = creators)
    testCHOMapping(choData, dcTerms.creator, creators)
  }

  it should "map collections" in {
    val collections = Seq(literal("rocks"), literal("socks"))
    val choData = ChoData(collections = collections)
    testCHOMapping(choData, dcTerms.isPartOf, collections)
  }

  it should "map publishers" in {
    val publishers = Seq(literal("mcgraw"), literal("hill"))
    val choData = ChoData(publishers = publishers)
    testCHOMapping(choData, dcTerms.publisher, publishers)
  }

  it should "map types" in {
    val types = Seq(literal("shiny"), literal("happy"))
    val choData = ChoData(types = types)
    testCHOMapping(choData, dcTerms.`type`, types)
  }

  it should "create an aggregation that's a bnode" in {
    assert(mapAggregation(aggregationData).isInstanceOf[BNode])
  }

  it should "create an aggregation of type ore:Aggregation" in {
    testAggegation(rdf.`type`, ore.Aggregation)
  }

  it should "create an aggregation containing the edm:aggregatedCHO" in {
    testAggegation(edm.aggregatedCHO, aggregationData.aggregatedCHO)
  }

  it should "create an aggregation containing the edm:isShownAt" in {
    testAggegation(edm.isShownAt, aggregationData.isShownAt)
  }

  it should "create an aggregation containing the edm:preview" in {
    testAggegation(edm.preview, aggregationData.preview.get)
  }

  it should "create an aggregation containing the edm:provider" in {
    testAggegation(edm.provider, aggregationData.provider)
  }

  it should "create an aggregation containing the dpla:originalRecord" in {
    testAggegation(dpla.originalRecord, aggregationData.originalRecord)
  }

  it should "create an aggregation containing the edm:dataProvider" in {
    testAggegation(edm.dataProvider, aggregationData.dataProvider)
  }

  it should "create a correct original record bnode" in {
    val originalRecord = mapOriginalRecord()
    val model = build()

    assert(originalRecord.isInstanceOf[BNode])
    assert(model.size === 1)
    assert(model.contains(originalRecord, rdf.`type`, edm.WebResource))
  }

  it should "create a correct data provider bnode" in {
    val dataProvider = mapDataProvider("foo")
    val model = build()

    assert(dataProvider.isInstanceOf[BNode])
    assert(model.size === 2)
    assert(model.contains(dataProvider, rdf.`type`, edm.Agent))
    assert(model.contains(dataProvider, dpla.providedLabel, literal("foo")))
  }

  it should "create a correct list of contributors" in {
    val creatorNames = Seq("huey", "dewey", "louie")
    val creators = mapCreators(creatorNames)
    val model = build()

    assert(creators.size === creatorNames.size)

    for (creator <- creators) {
      assert(creator.isInstanceOf[BNode])
      assert(model.contains(creator, rdf.`type`, edm.Agent))
      val creatorName =
        getProperty(model, creator, dpla.providedLabel)
          .orElse(literal("nope"))
          .stringValue()
      assert(creatorNames.contains(creatorName))
    }
  }

  it should "create a correct list of collections" in {
    val collectionNames = Seq("blue", "green", "white")
    val collections = mapCollections(collectionNames)
    val model = build()

    assert(collections.size === collectionNames.size)

    for (collection <- collections) {
      assert(collection.isInstanceOf[BNode])
      assert(model.contains(collection, rdf.`type`, dcmiType.Collection))
      val collectionName =
        getProperty(model, collection, dcTerms.title)
          .orElse(literal("nope"))
          .stringValue()
      assert(collectionNames.contains(collectionName))
    }
  }

  it should "create a correct list of dates" in {
    val dateStrings = Seq("2001-01-01", "1776-07-04")
    val dates = mapDates(dateStrings)
    val model = build()

    assert(dates.size === dateStrings.size)

    for (date <- dates) {
      assert(date.isInstanceOf[BNode])
      assert(model.contains(date, rdf.`type`, edm.TimeSpan))
      val dateText =
        getProperty(model, date, dpla.providedLabel)
          .orElse(literal("nope"))
          .stringValue()
      assert(dateStrings.contains(dateText))
    }
  }

  it should "assert that the item is an edm:WebResource" in {
    val itemIRI = iri("http://example.com")
    mapItemWebResource(itemIRI)
    val model = build()
    assert(model.size() === 1)
    assert(model.contains(itemIRI, rdf.`type`, edm.WebResource))
  }

  it should "assert that the contributing agent is an edm:Agent" in {
    val contributingAgent = iri("http://nypl.org")
    mapContributingAgent(contributingAgent, "NYPL")
    val model = build()
    assert(model.size() === 2)
    assert(model.contains(contributingAgent, rdf.`type`, edm.Agent))
    assert(model.contains(contributingAgent, skos.prefLabel, literal("NYPL")))
  }

  it should "assert that a thumbnail is an edm:WebResource" in {
    val thumbnail = iri("https://dp.la/assets/dpla-logo-3c3d47ca21e77d56645489606c391691.png")
    mapThumbWebResource(thumbnail)
    val model = build()
    assert(model.size() === 1)
    assert(model.contains(thumbnail, rdf.`type`, edm.WebResource))
  }

  it should "register the namespaces it's given" in {
    registerNamespaces(Seq(dpla, edm))
    val model = build()
    val namespaces = model.getNamespaces
    assert(namespaces.contains(dpla.ns))
    assert(namespaces.contains(edm.ns))
  }

  val aggregationData = AggregationData(
    aggregatedCHO = bnode("aggregatedCHO"),
    isShownAt = iri("https://bookface.com"),
    preview = Some(iri("http://hampsterdance.com")),
    provider = iri("https://dp.la"),
    bnode("originalRecord"),
    bnode("dataProvider")
  )

  private[this] def testAggegation(property: IRI, value: Value) = {
    val aggregation = mapAggregation(aggregationData)
    val properties = getProperties(build(), aggregation, property)

    assert(properties.size === 1)
    assert(properties.contains(value))
  }

  private[this] def testCHOMapping(choData: ChoData, property: IRI, values: Seq[Value]) = {
    val aggregatedCHO = mapAggregatedCHO(choData)
    val model = build()
    val results = getProperties(model, aggregatedCHO, property)

    assert(results.size === values.size)
    for (value <- values) assert(results.contains(value))
  }
}
