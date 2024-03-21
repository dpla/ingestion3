package dpla.ingestion3.mappers.rdf

import dpla.ingestion3.model.DplaMapData.{ZeroToMany, ZeroToOne}
import dpla.ingestion3.model.URI
import org.eclipse.rdf4j.model.{IRI, Model, Resource, Value}
import org.eclipse.rdf4j.model.util.ModelBuilder

trait RdfBuilderUtils extends RdfValueUtils with DefaultVocabularies {

  private val builder = new ModelBuilder()

  def registerNamespaces(vocabularies: Seq[Vocabulary]): Unit =
    for (vocabulary <- vocabularies)
      builder.setNamespace(vocabulary.ns)

  // sugar to set the subject as the resource but return a reference to the resource
  private def innerCreateResource(resource: Resource): Resource = {
    builder.subject(resource)
    resource
  }

  // convenience method for type coercion
  def createResource(uri: URI): Resource =
    innerCreateResource(iri(uri.value))

  def createResource(uri: String): Resource =
    innerCreateResource(iri(uri))

  // convenience method to avoid passing None
  def createResource(): Resource =
    innerCreateResource(bnode())

  // convenience method for type coercion
  def createResource(option: ZeroToOne[URI]): Resource = {
    option match {
      case Some(uri) => innerCreateResource(iri(uri.value))
      case None      => innerCreateResource(bnode())
    }
  }

  def setType(iri: IRI): Unit =
    builder.add(rdf.`type`, iri)

  def map(predicate: IRI, value: Value): Unit =
    builder.add(predicate, value)

  def map(predicate: IRI, list: ZeroToMany[_]): Unit =
    for (item <- list) item match {
      case URI(value)    => builder.add(predicate, iri(value))
      case value: String => builder.add(predicate, literal(value))
    }

  def map(predicate: IRI, option: ZeroToOne[_]): Unit =
    option match {
      case Some(URI(value))    => builder.add(predicate, iri(value))
      case Some(value: String) => builder.add(predicate, literal(value))
      case _                   => ()
    }

  def build(): Model = builder.build()
}
