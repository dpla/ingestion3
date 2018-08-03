package dpla.ingestion3.mappers.providers

import java.net.URI

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.enrichments.normalizations.filters.{DigitalSurrogateBlockList, FormatTypeValuesBlockList}
import dpla.ingestion3.mappers.utils.{Document, IdMinter, Mapping, XmlExtractor}
import dpla.ingestion3.messages.{IngestMessage, IngestMessageTemplates, MessageCollector}
import dpla.ingestion3.model.DplaMapData.{ExactlyOne, LiteralOrUri, ZeroToMany, ZeroToOne}
import dpla.ingestion3.model.{nameOnlyAgent, _}
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.util.{Failure, Success, Try}
import scala.xml._


class EsdnMapping extends Mapping[NodeSeq] with XmlExtractor with IdMinter[NodeSeq] with IngestMessageTemplates {

  val formatBlockList: Set[String] =
    DigitalSurrogateBlockList.termList ++
      FormatTypeValuesBlockList.termList

  // ID minting functions
  override def useProviderName(): Boolean = true

  override def getProviderName(): String = "esdn"

  override def getProviderId(implicit data: Document[NodeSeq]): String = // TODO confirm w/gretchen
    extractString(data \ "header" \ "identifier")
      .getOrElse(throw new RuntimeException(s"No ID for record $data")
      )

  def getByAtt(e: Elem, att: String, value: String) = {
    def filterAtribute(node: Node, att: String, value: String) =  (node \ ("@" + att)).text == value
    e \\ "_" filter { n=> filterAtribute(n, att, value)}
  }

  // SourceResource mapping
  override def alternateTitle(data: Document[NodeSeq]): ZeroToMany[String] =
    (data \\ "titleInfo")
      .map(node => getByAtt(node.asInstanceOf[Elem], "type", "alternative"))
      .flatMap(altTitle => extractStrings(altTitle \ "title"))

  override def collection(data: Document[NodeSeq]): Seq[DcmiTypeCollection] = // done
    (data \\ "relatedItem")
      .flatMap(node => getByAtt(node.asInstanceOf[Elem], "displayLabel", "collection"))
      .flatMap(node => getByAtt(node.asInstanceOf[Elem], "type", "host"))
      .flatMap(collection => extractStrings(collection \ "titleInfo" \ "title"))
      .map(nameOnlyCollection)

  override def contributor(data: Document[NodeSeq]): Seq[EdmAgent] = // done
    (data \\ "name")
      .filter(node => (node \ "role" \ "roleTerm").text.equalsIgnoreCase("contributor"))
      .flatMap(n => extractStrings(n \ "namePart"))
      .map(nameOnlyAgent)

  override def creator(data: Document[NodeSeq]): Seq[EdmAgent] =
    (data \\ "name")
      .filter(node => (node \ "role" \ "roleTerm").text.equalsIgnoreCase("creator"))
      .flatMap(n => extractStrings(n \ "namePart"))
      .map(nameOnlyAgent)

  override def date(data: Document[NodeSeq]): Seq[EdmTimeSpan] = {
    // Get dateCreated values with only a keyDate attribute
    val dateCreated = (data \ "originInfo" \\ "dateCreated")
      .flatMap(node => getByAtt(node.asInstanceOf[Elem], "keyDate", "yes"))
      .filter(node => node.attributes.get("point").isEmpty)
      .flatMap(node => extractStrings(node))
    // Get dateCreated values with a keyDate=yes attribute and point=start attribute
    val earlyDate = (data \ "originInfo" \\ "dateCreated")
      .flatMap(node => getByAtt(node.asInstanceOf[Elem], "keyDate", "yes"))
      .flatMap(node => getByAtt(node.asInstanceOf[Elem], "point", "start"))
      .flatMap(node => extractStrings(node))
    // Get dateCreated values with point=end attribute
    val lateDate = (data \ "originInfo" \\ "dateCreated")
      .flatMap(node => getByAtt(node.asInstanceOf[Elem], "point", "end"))
      .flatMap(node => extractStrings(node))

    (dateCreated.nonEmpty, earlyDate.nonEmpty, lateDate.nonEmpty) match {
      case (true, _, _) => dateCreated.map(stringOnlyTimeSpan)
      case (false, true, true) => Seq(EdmTimeSpan(
        originalSourceDate = Some(s"${earlyDate.head}-${lateDate.head}"),
        begin = earlyDate.headOption,
        end = lateDate.headOption
      ))
      case _ => Seq()
    }
  }

  override def description(data: Document[NodeSeq]): Seq[String] =
    (data \\ "note")
      .flatMap(node => getByAtt(node.asInstanceOf[Elem], "type", "content"))
      .flatMap(node => extractStrings(node))

  override def extent(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(data \\ "physicalDescription" \ "extent")

  override def format(data: Document[NodeSeq]): Seq[String] =
    (extractStrings(data \\ "physicalDescription" \\ "form") ++
      extractStrings(data \\ "genre") ++
        extractStrings(data \\ "typeOfResource"))
      .map(_.applyBlockFilter(formatBlockList))
      .filter(_.nonEmpty)

  override def identifier(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \\ "identifier")

  override def language(data: Document[NodeSeq]): Seq[SkosConcept] =
    extractStrings(data \\ "language" \\ "languageTerm")
      .map(nameOnlyConcept)

  override def place(data: Document[NodeSeq]): Seq[DplaPlace] =
    extractStrings(data \\ "subject" \\ "geographic")
      .map(nameOnlyPlace)

  override def publisher(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractStrings(data \ "originInfo" \\ "publisher")
      .map(nameOnlyAgent)

  override def relation(data: Document[NodeSeq]): ZeroToMany[LiteralOrUri] =
    extractStrings(data \\ "relatedItem" \\ "titleInfo" \\ "title")
      .map(eitherStringOrUri)

  override def subject(data: Document[NodeSeq]): Seq[SkosConcept] =
    extractStrings(data \\ "subject" \\ "topic")
      .map(nameOnlyConcept)

  override def temporal(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
    extractStrings(data \\ "subject" \\ "temporal").map(stringOnlyTimeSpan)

  override def title(data: Document[NodeSeq]): Seq[String] =
    (data \\ "metadata" \ "mods" \ "titleInfo")
      .filter(node => node.attributes.isEmpty)
      .flatMap(node => extractStrings(node \ "title"))

  override def `type`(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \ "metadata" \ "mods" \ "typeOfResource")

  // OreAggregation
  override def dplaUri(data: Document[NodeSeq]): URI = mintDplaItemUri(data)

  override def dataProvider(data: Document[NodeSeq])
                           (implicit msgCollector: MessageCollector[IngestMessage]): EdmAgent =
    (data \ "metadata" \ "mods" \ "note")
      .flatMap(node => getByAtt(node.asInstanceOf[Elem], "type", "ownership"))
      .flatMap(extractStrings)
      .map(nameOnlyAgent)
      .headOption match {
        case Some(s) => s
        case _ =>
          msgCollector.add(missingRequiredError(getProviderId(data), "dataProvider"))
          nameOnlyAgent("") // FIXME this shouldn't have to return an empty value.
      }

  override def edmRights(data: Document[NodeSeq]): ZeroToOne[URI] = {
    val rightsUris =
      (data \ "metadata" \ "mods" \ "accessCondition")
        .flatMap(node => getByAtt(node.asInstanceOf[Elem], "type", "use and reproduction"))

      rightsUris
        .flatMap(node => node.attribute("xlink"))
        .flatMap(n => extractString(n.head))
        .headOption match {
          case Some(uri) => Some(Utils.createUri(uri))
          case _ => None
      }
  }

  override def intermediateProvider(data: Document[NodeSeq]): ZeroToOne[EdmAgent] =
    (data \ "metadata" \ "mods" \ "note")
      .flatMap(node => getByAtt(node.asInstanceOf[Elem], "type", "regional council"))
      .flatMap(extractStrings)
      .map(nameOnlyAgent)
      .headOption // take the first value

  override def isShownAt(data: Document[NodeSeq])
                        (implicit msgCollector: MessageCollector[IngestMessage]): EdmWebResource =
        (data \ "metadata" \ "mods" \ "location" \ "url")
          .flatMap(node => getByAtt(node.asInstanceOf[Elem], "usage", "primary display"))
          .flatMap(node => getByAtt(node.asInstanceOf[Elem], "access", "object in context"))
          .flatMap(extractStrings)
          .flatMap(uriStr => {
            Try { new URI(uriStr)} match {
              case Success(uri) => Option(uriOnlyWebResource(uri))
              case Failure(f) =>
                msgCollector.add(mintUriError(id = getProviderId(data), field = "isShownAt", value = uriStr))
                None
            }
          }).headOption match {
            case None =>
              msgCollector.add(missingRequiredError(id = getProviderId(data), field = "isShownAt")) // record error message
              uriOnlyWebResource(new URI("")) // TODO Fix this -- it requires an Exception thrown or empty EdmWebResource
            case Some(s) => s
          }

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] = Utils.formatXml(data)

  override def preview(data: Document[NodeSeq])
                      (implicit msgCollector: MessageCollector[IngestMessage]): ZeroToOne[EdmWebResource] =
    (data \ "metadata" \ "mods" \ "location" \ "url")
      .flatMap(node => getByAtt(node.asInstanceOf[Elem], "usage", "preview"))
      .flatMap(extractStrings)
      .headOption match {
        case Some(s: String) => Some(uriOnlyWebResource(Utils.createUri(s)))
        case _ => None
      }

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = agent

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(data))

  // Helper method
  def agent = EdmAgent(
    name = Some("Empire State Digital Network"),
    uri = Some(Utils.createUri("http://dp.la/api/contributor/esdn"))
  )
}
