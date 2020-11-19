package dpla.ingestion3.mappers.providers

import java.net.URL

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.enrichments.normalizations.filters.{DigitalSurrogateBlockList, ExtentIdentificationList, FormatTypeValuesBlockList}
import dpla.ingestion3.mappers.utils.{Document, XmlExtractor, XmlMapping}
import dpla.ingestion3.messages.IngestMessageTemplates
import dpla.ingestion3.model.DplaMapData.{ExactlyOne, LiteralOrUri, ZeroToMany, ZeroToOne}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.util.{Failure, Success, Try}
import scala.xml._

class OrbisCascadeMapping extends XmlMapping with XmlExtractor with IngestMessageTemplates {

  /**
    * Fixes
    * - both freetext rights and edmRights are mapped to the same field
    * - dc:relation contains both collection title and URLs
    * - subject -- split on ;
    * - dc:identifier includes isShownAt value and also local ids
    *
    */

  // ID minting functions
  override def useProviderName(): Boolean = true

  override def getProviderName(): String = "orbis-cascade"

  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] =
    extractString(data \ "header" \ "identifier")

  // SourceResource mapping

  // TODO FIX
//    override def collection(data: Document[NodeSeq]): Seq[DcmiTypeCollection] =
//      extractStrings(metadataRoot(data) \ "relation")
//        .map(nameOnlyCollection)

    override def contributor(data: Document[NodeSeq]): Seq[EdmAgent] =
      extractStrings(metadataRoot(data) \ "contributor")
        .map(nameOnlyAgent)

  // done
  override def creator(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractStrings(metadataRoot(data) \ "creator")
      .map(nameOnlyAgent)

  // done
  override def date(data: Document[NodeSeq]): Seq[EdmTimeSpan] =
    extractStrings(metadataRoot(data) \ "date")
      .map(stringOnlyTimeSpan)

  // done
  override def description(data: Document[NodeSeq]): Seq[String] =
    extractStrings(metadataRoot(data) \ "description")

//  override def format(data: Document[NodeSeq]): Seq[String] =
//    extractStrings(data \ "metadata" \\ "format")
//      .flatMap(_.splitAtDelimiter(";"))
//      .map(_.applyBlockFilter(
//        DigitalSurrogateBlockList.termList ++
//          FormatTypeValuesBlockList.termList ++
//          ExtentIdentificationList.termList))
//      .filter(_.nonEmpty)

  // done
  // todo fix issue with isShownAt property
  override def identifier(data: Document[NodeSeq]): Seq[String] =
    extractStrings(metadataRoot(data) \ "identifier")

  // done
  override def language(data: Document[NodeSeq]): Seq[SkosConcept] =
    extractStrings(metadataRoot(data) \ "language")
      .map(nameOnlyConcept)

  // done
  override def publisher(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractStrings(metadataRoot(data) \ "publisher")
      .map(nameOnlyAgent)

  // done
  override def place(data: Document[NodeSeq]): Seq[DplaPlace] =
    extractStrings(metadataRoot(data) \ "coverage")
      .map(nameOnlyPlace)

  // done
  override def subject(data: Document[NodeSeq]): Seq[SkosConcept] =
    extractStrings(metadataRoot(data) \ "subject")
      .map(nameOnlyConcept)

  // done
  override def title(data: Document[NodeSeq]): Seq[String] =
    extractStrings(metadataRoot(data) \ "title")

  // done
  override def `type`(data: Document[NodeSeq]): Seq[String] =
    extractStrings(metadataRoot(data) \ "type")

  // OreAggregation
  override def dplaUri(data: Document[NodeSeq]): ZeroToOne[URI] = mintDplaItemUri(data)

  // TODO FIXME
  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    extractStrings(metadataRoot(data) \ "source")
      .map(nameOnlyAgent)
    // publisher(data) // Seq("TBD").map(nameOnlyAgent)

  // done
  override def edmRights(data: Document[NodeSeq]): ZeroToMany[URI] =
    rightsHelper(data)
      .filter(URI(_).validate)
      .map(URI)

  override def rights(data: Document[NodeSeq]): Seq[String] =
    rightsHelper(data)
      .filterNot(URI(_).validate)

  // fixme expected isShownAt is mapped to dc:identifier
  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    extractStrings(metadataRoot(data) \ "identifier")
      .filter(t => {
        Try { new URL(t) } match {
          case Success(_) => true
          case Failure(_) => false
        }
      })
      .map(stringOnlyWebResource)

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] = Utils.formatXml(data)

  // fixme nothing looks like a thumbnail
  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    extractStrings(data \ "metadata" \\ "preview")
      .map(stringOnlyWebResource)

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = agent

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(data))

  // Helper method
  def agent = EdmAgent(
    name = Some("Oribis Cascade"), // FIXME
    uri = Some(URI("http://dp.la/api/contributor/ohio"))
  )

  /**
    * Extracts values from format field and returns values that appear to be
    * extent statements
    *
    * @param data
    * @return
    */
  def extentFromFormat(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(data \ "metadata" \\ "format")
      .flatMap(_.splitAtDelimiter(";"))
      .map(_.extractExtents)
      .filter(_.nonEmpty)

  def metadataRoot(data: Document[NodeSeq]): NodeSeq = data \ "metadata" \ "dc"

  def rightsHelper(data: Document[NodeSeq]): Seq[String] = extractStrings(metadataRoot(data) \"rights")
}
