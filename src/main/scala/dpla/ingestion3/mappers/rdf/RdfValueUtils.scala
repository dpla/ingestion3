package dpla.ingestion3.mappers.rdf

import org.eclipse.rdf4j.model._
import org.eclipse.rdf4j.model.impl.SimpleValueFactory

import RdfValueUtils._
/**
  * Mixin for conveniently creating RDF4J datamodel objects. Maintains a SimpleValueFactory singleton so they're not
  * being created all over the place.
  *
  */

trait RdfValueUtils {

  def literal(string: String): Literal =
    valueFactory.createLiteral(string)

  def iri(uri: java.net.URI): IRI =
    valueFactory.createIRI(uri.toString)

  def iri(namespace: String, uri: java.net.URI): IRI =
    valueFactory.createIRI(namespace, uri.toString)


  def iri(string: String): IRI =
    valueFactory.createIRI(string)

  def iri(namespace: String, localName: String): IRI =
    valueFactory.createIRI(namespace, localName)

  def bnode(): BNode =
    valueFactory.createBNode()

  def bnode(nodeId: String): BNode =
    valueFactory.createBNode(nodeId)

  def stmt(subj: Resource, pred: IRI, obj: Value): Statement =
    valueFactory.createStatement(subj, pred, obj)

}

object RdfValueUtils {
  private lazy val valueFactory = SimpleValueFactory.getInstance()
}

