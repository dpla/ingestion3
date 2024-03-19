package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.enrichments.normalizations.filters.{DigitalSurrogateBlockList, FormatTypeValuesBlockList}
import dpla.ingestion3.mappers.utils.{Document, XmlMapping, XmlExtractor}
import dpla.ingestion3.messages.IngestMessageTemplates
import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model.{nameOnlyAgent, _}
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.xml._


class EsdnMapping extends XmlMapping with XmlExtractor with IngestMessageTemplates {

  val formatBlockList: Set[String] =
    DigitalSurrogateBlockList.termList ++
      FormatTypeValuesBlockList.termList

  // ID minting functions
  override def useProviderName: Boolean = true

  override def getProviderName: Option[String] = Some("esdn")

  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] =
    extractString(data \ "header" \ "identifier")

  // SourceResource mapping
  override def alternateTitle(data: Document[NodeSeq]): ZeroToMany[String] =
    (data \\ "titleInfo")
      .map(node => getByAttribute(node.asInstanceOf[Elem], "type", "alternative"))
      .flatMap(altTitle => extractStrings(altTitle \ "title"))

  override def collection(data: Document[NodeSeq]): Seq[DcmiTypeCollection] =
    (data \\ "relatedItem")
      .flatMap(node => getByAttribute(node.asInstanceOf[Elem], "displayLabel", "collection"))
      .flatMap(node => getByAttribute(node.asInstanceOf[Elem], "type", "host"))
      .flatMap(collection => extractStrings(collection \ "titleInfo" \ "title"))
      .map(nameOnlyCollection)

  override def contributor(data: Document[NodeSeq]): Seq[EdmAgent] =
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
    val dateCreated = (data \\ "originInfo" \\ "dateCreated")
      .flatMap(node => getByAttribute(node.asInstanceOf[Elem], "keyDate", "yes"))
      .filter(node => node.attributes.get("point").isEmpty)
      .flatMap(node => extractStrings(node))
    // Get dateCreated values with a keyDate=yes attribute and point=start attribute
    val earlyDate = (data \\ "originInfo" \\ "dateCreated")
      .flatMap(node => getByAttribute(node.asInstanceOf[Elem], "keyDate", "yes"))
      .flatMap(node => getByAttribute(node.asInstanceOf[Elem], "point", "start"))
      .flatMap(node => extractStrings(node))
    // Get dateCreated values with point=end attribute
    val lateDate = (data \\ "originInfo" \\ "dateCreated")
      .flatMap(node => getByAttribute(node.asInstanceOf[Elem], "point", "end"))
      .flatMap(node => extractStrings(node))

    (dateCreated.nonEmpty, earlyDate.nonEmpty, lateDate.nonEmpty) match {
      case (true, _, _) => dateCreated.map(stringOnlyTimeSpan)
      case (false, true, true) => Seq(EdmTimeSpan(
        originalSourceDate = Some(s"${earlyDate.headOption.getOrElse("")}-${lateDate.headOption.getOrElse("")}"),
        begin = earlyDate.headOption,
        end = lateDate.headOption
      ))
      case _ => Seq()
    }
  }

  override def description(data: Document[NodeSeq]): Seq[String] =
    (data \\ "note")
      .flatMap(node => getByAttribute(node.asInstanceOf[Elem], "type", "content"))
      .flatMap(node => extractStrings(node))

  override def extent(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(data \\ "physicalDescription" \ "extent")

  override def format(data: Document[NodeSeq]): Seq[String] =
    (extractStrings(data \\ "physicalDescription" \\ "form") ++
      extractStrings(data \\ "genre") ++
      extractStrings(data \\ "typeOfResource"))
      .flatMap(_.splitAtDelimiter(";"))
      .map(_.applyBlockFilter(formatBlockList))
      .filter(_.nonEmpty)

  override def identifier(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \ "metadata" \ "mods" \ "identifier")

  override def language(data: Document[NodeSeq]): Seq[SkosConcept] =
    extractStrings(data \\ "language" \\ "languageTerm")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyConcept)

  override def place(data: Document[NodeSeq]): Seq[DplaPlace] =
    extractStrings(data \\ "subject" \\ "geographic")
      .map(nameOnlyPlace)

  override def publisher(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractStrings(data \\ "originInfo" \\ "publisher")
      .map(nameOnlyAgent)

  override def relation(data: Document[NodeSeq]): ZeroToMany[LiteralOrUri] =
    extractStrings(data \\ "relatedItem" \\ "titleInfo" \\ "title")
      .map(eitherStringOrUri)
      .distinct

  //  <accessCondition> when the @type="use and reproduction" attribute is not present
  override def rights(data: Document[NodeSeq]): AtLeastOne[String] =
    (data \ "metadata" \ "mods" \ "accessCondition")
      .filterNot({ n => filterAttribute(n, "type", "use and reproduction") })
      .flatMap(extractStrings)

  override def subject(data: Document[NodeSeq]): Seq[SkosConcept] =
    (extractStrings(data \\ "subject" \\ "topic") ++
      extractStrings(data \\ "subject" \\ "name") ++
      extractStrings(data \\ "subject" \ "titleInfo" \ "title")
      ).map(nameOnlyConcept)

  override def temporal(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
    extractStrings(data \\ "subject" \\ "temporal").map(stringOnlyTimeSpan)

  override def title(data: Document[NodeSeq]): Seq[String] =
    (data \ "metadata" \ "mods" \ "titleInfo")
      .filter(node => node.attributes.isEmpty)
      .flatMap(node => extractStrings(node \ "title"))

  override def `type`(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \ "metadata" \ "mods" \ "typeOfResource")

  // OreAggregation
  override def dplaUri(data: Document[NodeSeq]): ZeroToOne[URI] = mintDplaItemUri(data)

  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    (data \ "metadata" \ "mods" \ "note")
      .flatMap(node => getByAttribute(node.asInstanceOf[Elem], "type", "ownership"))
      .flatMap(extractStrings)
      .map(nameOnlyAgent)

  override def edmRights(data: Document[NodeSeq]): ZeroToMany[URI] =
    (data \ "metadata" \ "mods" \ "accessCondition")
      .flatMap(node => getByAttribute(node.asInstanceOf[Elem], "type", "use and reproduction"))
      .flatMap(node => node.attribute(node.getNamespace("xlink"), "href"))
      .flatMap(n => extractString(n.head))
      .map(URI)

  override def intermediateProvider(data: Document[NodeSeq]): ZeroToOne[EdmAgent] =
    (data \ "metadata" \ "mods" \ "note")
      .flatMap(node => getByAttribute(node.asInstanceOf[Elem], "type", "regional council"))
      .flatMap(extractStrings)
      .map(nameOnlyAgent)
      .headOption // take the first value

  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    (data \ "metadata" \ "mods" \ "location" \ "url")
      .flatMap(node => getByAttribute(node.asInstanceOf[Elem], "usage", "primary display"))
      .flatMap(node => getByAttribute(node.asInstanceOf[Elem], "access", "object in context"))
      .flatMap(extractStrings)
      .map(stringOnlyWebResource)

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] = Utils.formatXml(data)

  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    (data \ "metadata" \ "mods" \ "location" \ "url")
      .flatMap(node => getByAttribute(node.asInstanceOf[Elem], "access", "preview"))
      .flatMap(extractStrings)
      .map(stringOnlyWebResource)

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = agent

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(data))

  // Helper method
  def agent: EdmAgent = EdmAgent(
    name = Some("Empire State Digital Network"),
    uri = Some(URI("http://dp.la/api/contributor/esdn"))
  )
}

