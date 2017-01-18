package la.dp.ingestion3.mappings


import java.io.ByteArrayOutputStream
import org.apache.jena.rdf.model.{ModelFactory, Resource}
import org.apache.jena.vocabulary._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.jena.rdf.model.ResourceFactory._
import org.apache.jena.riot.{Lang, RDFDataMgr}

trait CdlMapping {

  val ITEM_URL_PREFIX = "https://thumbnails.calisphere.org/clip/150x150/"
  val CDL_URI = "http://dp.la/api/contributor/cdl"
  val EDM = "http://www.europeana.eu/schemas/edm/"
  val MAP = "http://dp.la/about/map/"
  val ORE = "http://www.openarchives.org/ore/terms/"

  def map(inputData: String): String = {

    val json = parse(inputData)
    val model = ModelFactory.createDefaultModel

    val edmWebResource = createResource(EDM + "WebResource")

    val item = (json \ "url_item").toOption match {
      case Some(JString(url)) =>
        val tmp = model.getResource(url)
        tmp.addProperty(RDF.`type`, edmWebResource)
      case _ => throw new IllegalStateException("Record has no 'url_item' field.")
    }

    val edmAgent = createResource(EDM + "Agent")
    val cdl = model.getResource(CDL_URI)
    cdl.addProperty(RDF.`type`, edmAgent)
    cdl.addProperty(SKOS.prefLabel, "California Digital Library")

    val thumb = json \ "reference_image_md5" match {
      case JString(imageMD5) =>
        val thumb = model.getResource(ITEM_URL_PREFIX + imageMD5)
        thumb.addProperty(RDF.`type`, edmWebResource)
        Some(thumb)
      case _ => None
    }

    val root = model.createResource
    root.addProperty(RDF.`type`, createResource(ORE + "Aggregation"))

    val originalRecord = model.createResource
    root.addProperty(createProperty(MAP + "originalRecord"), originalRecord)
    originalRecord.addProperty(RDF.`type`, edmWebResource)

    val aggregatedCHO = model.createResource
    root.addProperty(createProperty(EDM + "aggregatedCHO"), aggregatedCHO)
    aggregatedCHO.addProperty(RDF.`type`, createResource(MAP + "SourceResource"))

    for (JString(title) <- json \ "title_ss") {
      aggregatedCHO.addProperty(DCTerms.title, title)
    }

    for (JString(dateValue) <- json \ "date_ss") {
      val date: Resource = model.createResource
      aggregatedCHO.addProperty(DC_11.date, date)
      date.addProperty(RDF.`type`, createResource(EDM + "TimeSpan"))
      date.addProperty(createProperty(MAP + "providedLabel"), dateValue)
    }

    for (JString(identifierValue) <- json \ "identifier_ss") {
      aggregatedCHO.addProperty(DC_11.identifier, identifierValue)
    }

    aggregatedCHO.addProperty(DC_11.identifier, item)

    for (JString(rightsValue) <- json \ "rights_ss") {
      aggregatedCHO.addProperty(DC_11.rights, rightsValue)
    }

    for (JString(contributorValue) <- json \ "contributor_ss") {
      val contributor: Resource = model.createResource
      aggregatedCHO.addProperty(DCTerms.contributor, contributor)
      contributor.addProperty(RDF.`type`, edmAgent)
      contributor.addProperty(createProperty(MAP + "providedLabel"), contributorValue)
    }

    for (JString(creatorValue) <- json \ "creator_ss") {
      val creator: Resource = model.createResource
      aggregatedCHO.addProperty(DCTerms.creator, creatorValue)
      creator.addProperty(RDF.`type`, edmAgent)
      creator.addProperty(createProperty(MAP + "providedLabel"), creatorValue)
    }

    for (JString(collectionValue) <- json \ "collection_name") {
      val collection: Resource = model.createResource
      aggregatedCHO.addProperty(DCTerms.isPartOf, collection)
      collection.addProperty(RDF.`type`, DCTypes.Collection)
      collection.addProperty(DCTerms.title, collectionValue)
    }

    for (JString(publisherValue) <- json \ "publisher_ss") {
      aggregatedCHO.addProperty(DCTerms.publisher, publisherValue)
    }

    for (JString(typeValue) <- json \ "type") {
      aggregatedCHO.addProperty(DCTerms.`type`, typeValue)
    }

    val campusJson = (json \ "campus_name").children.headOption
    val repositoryJson = (json \ "repository_name").children.headOption

    (campusJson, repositoryJson) match {
      case (Some(JString(campus)), Some(JString(repository))) =>
        val providerString = campus + ", " + repository
        val provider = model.createResource
        provider.addProperty(RDF.`type`, edmAgent)
        provider.addProperty(createProperty(MAP + "providedLabel"), providerString)
        root.addProperty(createProperty(EDM + "dataProvider"), provider)

      case (None, Some(JString(repository))) =>
        val provider: Resource = model.createResource
        provider.addProperty(RDF.`type`, edmAgent)
        provider.addProperty(createProperty(MAP + "providedLabel"), repository)
        root.addProperty(createProperty(EDM + "dataProvider"), provider)
    }

    (json \ "url_item").toOption match {
      case Some(JString(value)) => root.addProperty(createProperty(EDM + "isShownAt"), value)
    }

    thumb match {
      case Some(value) => root.addProperty(createProperty(EDM + "preview"), value)
      case None => Unit
    }

    val out: ByteArrayOutputStream = new ByteArrayOutputStream
    RDFDataMgr.write(out, model, Lang.TURTLE)
    out.toString
  }
}
