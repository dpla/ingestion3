package dpla.ingestion3.mappers.utils

import java.net.URI

import dpla.ingestion3.mappers.XmlMapper
import dpla.ingestion3.mappers.providers.PaMapping
import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model._
import dpla.ingestion3.model.OreAggregation
import org.json4s.JValue

import scala.util.Try
import scala.xml.NodeSeq

trait Parser[T] {
  def parse(data: String): Document[T]
}

trait Mapper[T, E] {
  def map(document: Document[T], mapping: Mapping[T]): Try[OreAggregation]
}

case class Document[T](value: T) {
  def get: T = value
}

//noinspection TypeAnnotation
trait Mapping[T] {

  implicit def unwrap(document: Document[T]): T = document.get

  // OreAggregation
  def dplaUri(data: T): ExactlyOne[URI] //TODO IS THIS DOCUMENT[T] INSTEAD
  def dataProvider(data: T): ExactlyOne[EdmAgent]
  def originalRecord(data: T): ExactlyOne[String]
  def hasView(data: T): ZeroToMany[EdmWebResource] = Seq()
  def intermediateProvider(data: T): ZeroToOne[EdmAgent] = None
  def isShownAt(data: T): ExactlyOne[EdmWebResource]
  def `object`(data: T): ZeroToOne[EdmWebResource] = None // full size image
  def preview(data: T): ZeroToOne[EdmWebResource] = None // thumbnail
  def provider(data: T): ExactlyOne[EdmAgent]
  def edmRights(data: T): ZeroToOne[URI] = None
  def sidecar(data: T): JValue

  // SourceResource
  def alternateTitle(data: T): ZeroToMany[String] = Seq()
  def collection(data: T): ZeroToMany[DcmiTypeCollection] = Seq()
  def contributor(data: T): ZeroToMany[EdmAgent] = Seq()
  def creator(data: T): ZeroToMany[EdmAgent] = Seq()
  def date(data: T): ZeroToMany[EdmTimeSpan] = Seq()
  def description(data: T): ZeroToMany[String] = Seq()
  def extent(data: T): ZeroToMany[String] = Seq()
  def format(data: T): ZeroToMany[String] = Seq()
  def genre(data: T): ZeroToMany[SkosConcept] = Seq()
  def identifier(data: T): ZeroToMany[String] = Seq()
  def language(data: T): ZeroToMany[SkosConcept] = Seq()
  def place(data: T): ZeroToMany[DplaPlace] = Seq()
  def publisher(data: T): ZeroToMany[EdmAgent] = Seq()
  def relation(data: T): ZeroToMany[LiteralOrUri] = Seq()
  def replacedBy(data: T): ZeroToMany[String] = Seq()
  def replaces(data: T): ZeroToMany[String] = Seq()
  def rights(data: T): AtLeastOne[String] = Seq()
  def rightsHolder(data: T): ZeroToMany[EdmAgent] = Seq()
  def subject(data: T): ZeroToMany[SkosConcept] = Seq()
  def temporal(data: T): ZeroToMany[EdmTimeSpan] = Seq()
  def title(data: T): AtLeastOne[String] = Seq()
  def `type`(data: T): ZeroToMany[String] = Seq()
}

trait IngestionProfile[T] {

  def getParser: Parser[T]
  def getMapper: Mapper[T, Mapping[T]]
  def getMapping: Mapping[T]

  def performMapping(data: String): Try[OreAggregation] = {
    val parser = getParser
    val mapping = getMapping
    val mapper = getMapper

    val document = parser.parse(data)
    mapper.map(document, mapping)
  }
}

trait XmlProfile extends IngestionProfile[Document[NodeSeq]] {
  override def getParser = new XmlParser
  override def getMapper = ??? //new XmlMapper
}

class PaProfile extends XmlProfile {
  type Mapping = PaMapping
  override def getMapping = new PaMapping
}

//
////the main() method for running a mapping
//object MappingExecutor extends App {
//  import IngestionOps._
//  val profile: PaProfile = new PaProfile() // FIXME -- see psudo code for diff
//
//  val oreAggregation: Try[OreAggregation] = new MappingWorkflow().performMapping("")
//}
//

