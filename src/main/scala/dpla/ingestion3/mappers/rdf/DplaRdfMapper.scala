package dpla.ingestion3.mappers.rdf


import dpla.ingestion3.model._
import org.eclipse.rdf4j.model.{Model, Resource}

/**
  * This class handles mapping DplaMapData to the RDF4J Model domain
  * @param doc DPLA MAP Record to express as RDF
  */

class DplaRdfMapper(doc: OreAggregation) extends RdfBuilderUtils {

  def map(): Model = {
    assert(true) //todo assertions
    registerNamespaces(defaultVocabularies)
    mapSourceResource()
    mapOreAggregation()
    build()
  }

  // Handling Core Classes

  def mapSourceResource(): Unit = {
    val sourceResource = doc.sourceResource
    createResource(doc.dplaUri.value + "#sourceResource")
    setType(dpla.SourceResource)
    map(dc.date, sourceResource.date.map(edmTimespan))
    map(dcTerms.title, sourceResource.title)
    map(dc.identifier, sourceResource.identifier)
    map(dc.rights, sourceResource.rights)
    map(dcTerms.contributor, edmAgent(sourceResource.contributor))
    map(dcTerms.creator, edmAgent(sourceResource.creator))
    map(dcTerms.isPartOf, sourceResource.collection.map(dcmiTypeCollection))
    map(dcTerms.publisher, sourceResource.publisher)
    map(dcTerms.`type`, sourceResource.`type`)
  }

  def mapOreAggregation(): Unit = {
    val oreAggregation = doc
    createResource(oreAggregation.dplaUri)
    setType(ore.Aggregation)
    //links the SourceResource in the record to the Aggregation
    map(edm.aggregatedCHO, iri(oreAggregation.dplaUri.toString + "#sourceResource"))
    //links the top level edm:WebResource to the Aggregation
    map(edm.isShownAt, iri(doc.isShownAt.uri.value))
    map(edm.provider, edmAgent(oreAggregation.provider))
    map(dpla.originalRecord, oreAggregation.originalRecord) //TODO what to do with ORs
    map(edm.dataProvider, edmAgent(oreAggregation.dataProvider))
    map(edm.hasView, oreAggregation.hasView)

    //big image
    if (oreAggregation.`object`.nonEmpty)
      map(edm.`object`, oreAggregation.`object`.map(edmWebResource))

    //thumbnail
    if (oreAggregation.preview.isDefined)
      map(edm.preview, edmWebResource(oreAggregation.preview.get))
  }

  // Context Classes

  //generic handling of edm:Agent
  private def edmAgent(agent: EdmAgent): Resource = {
    val resource = createResource(agent.uri)
    setType(edm.Agent)
    map(skos.prefLabel, agent.name)
    map(dpla.providedLabel, agent.providedLabel)
    map(skos.note, agent.note)
    map(skos.inScheme, agent.scheme)
    map(skos.exactMatch, agent.exactMatch)
    map(skos.closeMatch, agent.closeMatch)
    resource
  }

  //convenience method for multiple agents
  private def edmAgent(agents: Seq[EdmAgent]): Seq[Resource] =
    agents.map(agent => edmAgent(agent))


  //generic handling of edm:WebResource
  private def edmWebResource(webResource: EdmWebResource): Resource = {
    val resource = createResource(webResource.uri)
    setType(edm.WebResource)
    map(dc.format, webResource.fileFormat)
    map(dc.rights, webResource.dcRights)
    map(edm.rights, webResource.edmRights)
    resource
  }

  //generic handling of edm:TimeSpan
  private def edmTimespan(timeSpan: EdmTimeSpan): Resource = {
    val resource = createResource()
    setType(edm.TimeSpan)
    map(dpla.providedLabel, timeSpan.originalSourceDate)
    map(edm.begin, timeSpan.begin)
    map(edm.end, timeSpan.end)
    resource
  }

  //generic handling of dcmi:TypeCollection
  private def dcmiTypeCollection(dcmiTypeCollection: DcmiTypeCollection): Resource = {
    val resource = createResource()
    setType(dcmiType.Collection)
    map(dcTerms.title, dcmiTypeCollection.title)
    map(dcTerms.description, dcmiTypeCollection.description)
    resource
  }
}


