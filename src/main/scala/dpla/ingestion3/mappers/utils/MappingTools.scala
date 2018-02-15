package dpla.ingestion3.mappers.utils

import java.net.URI

import dpla.ingestion3.mappers.providers.PaMapping
import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model.{OreAggregation, _}
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
  def dplaUri(data: Document[T]): ExactlyOne[URI] //TODO IS THIS DOCUMENT[T] INSTEAD
  def dataProvider(data: Document[T]): ExactlyOne[EdmAgent]
  def originalRecord(data: Document[T]): ExactlyOne[String]
  def hasView(data: Document[T]): ZeroToMany[EdmWebResource] = Seq()
  def intermediateProvider(data: Document[T]): ZeroToOne[EdmAgent] = None
  def isShownAt(data: Document[T]): ExactlyOne[EdmWebResource]
  def `object`(data: Document[T]): ZeroToOne[EdmWebResource] = None // full size image
  def preview(data: Document[T]): ZeroToOne[EdmWebResource] = None // thumbnail
  def provider(data: Document[T]): ExactlyOne[EdmAgent]
  def edmRights(data: Document[T]): ZeroToOne[URI] = None
  def sidecar(data: Document[T]): JValue

  // SourceResource
  def alternateTitle(data: Document[T]): ZeroToMany[String] = Seq()
  def collection(data: Document[T]): ZeroToMany[DcmiTypeCollection] = Seq()
  def contributor(data: Document[T]): ZeroToMany[EdmAgent] = Seq()
  def creator(data: Document[T]): ZeroToMany[EdmAgent] = Seq()
  def date(data: Document[T]): ZeroToMany[EdmTimeSpan] = Seq()
  def description(data: Document[T]): ZeroToMany[String] = Seq()
  def extent(data: Document[T]): ZeroToMany[String] = Seq()
  def format(data: Document[T]): ZeroToMany[String] = Seq()
  def genre(data: Document[T]): ZeroToMany[SkosConcept] = Seq()
  def identifier(data: Document[T]): ZeroToMany[String] = Seq()
  def language(data: Document[T]): ZeroToMany[SkosConcept] = Seq()
  def place(data: Document[T]): ZeroToMany[DplaPlace] = Seq()
  def publisher(data: Document[T]): ZeroToMany[EdmAgent] = Seq()
  def relation(data: Document[T]): ZeroToMany[LiteralOrUri] = Seq()
  def replacedBy(data: Document[T]): ZeroToMany[String] = Seq()
  def replaces(data: Document[T]): ZeroToMany[String] = Seq()
  def rights(data: Document[T]): AtLeastOne[String] = Seq()
  def rightsHolder(data: Document[T]): ZeroToMany[EdmAgent] = Seq()
  def subject(data: Document[T]): ZeroToMany[SkosConcept] = Seq()
  def temporal(data: Document[T]): ZeroToMany[EdmTimeSpan] = Seq()
  def title(data: Document[T]): AtLeastOne[String] = Seq()
  def `type`(data: Document[T]): ZeroToMany[String] = Seq()
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

trait XmlProfile extends IngestionProfile[NodeSeq] {
  override def getParser = new XmlParser
  override def getMapper = ??? //new XmlMapper
}

class PaProfile extends XmlProfile {
  type Mapping = PaMapping

  override def getMapping = new PaMapping
}

//
//the main() method for running a mapping

object MappingExecutor extends App {
  // import IngestionOps._
  val profile: PaProfile = new PaProfile()

  // val oreAggregation: Try[OreAggregation] = new MappingWorkflow().performMapping("")

  val xml = <record><metadata><header><identifier>1234</identifier></header><rights>http://google.com</rights></metadata></record>
  val rights = profile.getMapping.edmRights(Document(xml))

  println(rights)
}


