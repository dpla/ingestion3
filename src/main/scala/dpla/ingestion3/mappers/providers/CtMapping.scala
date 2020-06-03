package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.filters.{DigitalSurrogateBlockList, FormatTypeValuesBlockList}
import dpla.ingestion3.mappers.utils.{Document, XmlExtractor, XmlMapping}
import dpla.ingestion3.messages.IngestMessageTemplates
import dpla.ingestion3.model.DplaMapData.{AtLeastOne, ExactlyOne, ZeroToMany, ZeroToOne}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.xml._

class CtMapping extends XmlMapping with XmlExtractor
  with IngestMessageTemplates {

  val formatBlockList: Set[String] =
    DigitalSurrogateBlockList.termList ++
      FormatTypeValuesBlockList.termList

  // ID minting functions
  override def useProviderName(): Boolean = false

  override def getProviderName(): String = "ct"

  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] =
    isShownAtStrings(data).headOption

  //  mods/titleInfo @type=alternative> children combined as follows (with a single space between):
  //  <nonSort> <title> <subTitle>  <partName> <partNumber>
  override def alternateTitle(data: Document[NodeSeq]): ZeroToMany[String] = {
    val altTitles = (data \ "titleInfo").flatMap(node => getByAttribute(node, "type", "alternative"))
    constructTitles(altTitles)
  }

  // mods/name/namePart when metadata/mods/name/role/roleTerm equals anything OTHER THAN "creator" (case insentive)
  override def contributor(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    (data \ "name")
      .filterNot(node => (node \ "role" \ "roleTerm").text.equalsIgnoreCase("creator"))
      .flatMap(n => extractStrings(n \ "namePart"))
      .map(nameOnlyAgent)

  // metadata/mods/name/namePart when metadata/mods/name/role/roleTerm equals anything OTHER THAN "creator" (case insentive)
  override def creator(data: Document[NodeSeq]): Seq[EdmAgent] =
    (data \ "name")
      .filter(node => (node \ "role" \ "roleTerm").text.equalsIgnoreCase("creator"))
      .flatMap(n => extractStrings(n \ "namePart"))
      .map(nameOnlyAgent)

  // metadata/mods/originInfo/dateIssued OR
  // metadata/mods/originInfo @point=start + metadata/mods/originInfo @point=end
  // (some records do not have the @point, but other indicate a start and end in separate fields)
  override def date(data: Document[NodeSeq]): Seq[EdmTimeSpan] = {
    val dateIssued = extractStrings(data \ "originInfo" \ "dateIssued")
      .map(stringOnlyTimeSpan)

    val startEndDates = (data \ "originInfo")
      .filter(n => (n \@ "start").nonEmpty && (n \@ "end").nonEmpty)
      .map(n => s"${n \@ "start"}-${n \@ "end"}")
      .map(stringOnlyTimeSpan)

    if(dateIssued.nonEmpty)
      dateIssued
    else
      startEndDates
  }

  // metadata/mods/abstract
  override def description(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \ "abstract")

  // metadata/mods/physicalDescription/extent + value of the @unit attribute
  override def extent(data: Document[NodeSeq]): ZeroToMany[String] =
    (data \ "physicalDescription" \ "extent").map(node => {
      s"${node.text} ${node \@ "unit"}".trim
    })

  // metadata/mods/genre
  // metadata/mods/physicalDescription/form
  // metadata/mods/subject/genre
  override def format(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \ "genre") ++
      extractStrings(data \ "physicalDescription" \ "form") ++
      extractStrings(data \ "subject" \ "genre")

  // metadata/mods/identifier
  override def identifier(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \ "identifier")

  // metadata/mods/language/languageTerm
  override def language(data: Document[NodeSeq]): Seq[SkosConcept] =
    extractStrings(data \ "language" \ "languageTerm")
      .map(nameOnlyConcept)

  // metadata/mods/subject/cartographics/coordinates AND metadata/mods/subject/geographic
  override def place(data: Document[NodeSeq]): Seq[DplaPlace] =
    (extractStrings(data \ "subject" \ "cartographics" \ "coordinates") ++ // FIXME Shouldn't coordinates get mapped differently?
      extractStrings(data \ "subject" \ "geographic"))
      .map(nameOnlyPlace)

  // metadata/mods/originInfo/publisher
  override def publisher(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractStrings(data \ "originInfo" \ "publisher")
      .map(nameOnlyAgent)

  // metadata/mods/accessCondition
  override def rights(data: Document[NodeSeq]): AtLeastOne[String] =
    extractStrings(data \ "accessCondition")

  // ALL CHILDREN of metadata/mods/subject/topic|name|titleInfo
  // TODO Confirm this mappping, unsure what 'all children' means here
  override def subject(data: Document[NodeSeq]): Seq[SkosConcept] =
    (extractStrings(data \ "subject" \ "topic") ++
      extractStrings(data \ "subject" \ "name") ++
      extractStrings(data \ "subject" \ "titleInfo"))
        .map(nameOnlyConcept)

  // metadata/mods/subject/temporal
  override def temporal(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
    extractStrings(data \ "subject" \ "temporal")
      .map(stringOnlyTimeSpan)

  // <titleInfo> children combined as follows (with a single space between):
  // <nonSort> <title> <subTitle>  <partName> <partNumber>
  // EXCEPT WHEN titleInfo @type = alternative"
  override def title(data: Document[NodeSeq]): Seq[String] = {
    val altTitles = (data \ "titleInfo").filterNot(node => filterAttribute(node, "type", "alternative"))
    constructTitles(altTitles)
  }

  // metadata/mods/genre
  // metadata/mods/physicalDescription/form
  // metadata/mods/subject/genre
  // metadata/mods/typeofresource
  override def `type`(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \ "genre") ++
      extractStrings(data \ "physicalDescription" \ "form") ++
      extractStrings(data \ "subject" \ "genre") ++
      extractStrings(data \ "typeOfResource")

  // OreAggregation
  override def dplaUri(data: Document[NodeSeq]): ZeroToOne[URI] = mintDplaItemUri(data)
  
  // <note type="ownership">
  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    (data \ "note").map(n => getByAttribute(n, "type", "ownership"))
      .flatMap(extractStrings)
      .map(nameOnlyAgent)

  override def edmRights(data: Document[NodeSeq]): ZeroToMany[URI] =
    (data \ "accessCondition")
      .flatMap(node => getByAttribute(node.asInstanceOf[Elem], "type", "use and reproduction"))
      .flatMap(node => node.attribute(node.getNamespace("xlink"), "href"))
      .flatMap(n => extractString(n.head))
      .map(URI)


  // <identifier type=”hdl>
  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    isShownAtStrings(data)
      .map(stringOnlyWebResource)

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] = Utils.formatXml(data)

  // Take the <identifier type=”hdl”> element, (e.g. http://hdl.handle.net/11134/20004:20073175)
  // Change http://hdl.handle.net/11134 into https://ctdigitalarchive.org/islandora/object
  // and then add the PID (20004:20073175) followed by /datastream/TN.
  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    isShownAtStrings(data)
      .map(isa => isa.replaceAllLiterally("http://hdl.handle.net/11134/", "https://ctdigitalarchive.org/islandora/object/"))
      .map(preview => preview + "/datastream/TN")
      .map(stringOnlyWebResource)

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = agent

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(data))

  // Helper method
  def agent = EdmAgent(
    name = Some("Connecticut Digital Archive"),
    uri = Some(URI("http://dp.la/api/contributor/ct"))
  )

  // Help functions
  def constructTitles(nodeSeq: NodeSeq): ZeroToMany[String] = {
    nodeSeq.map (node => {
      val nonSort = extractString( node \ "nonSort")
      val title = extractString( node \ "title")
      val subTitle = extractString( node \ "subTitle")
      val partName = extractString( node \ "partName")
      val partNumber = extractString( node \ "partNumber")
      List(nonSort, title, subTitle, partName, partNumber).flatten.mkString(" ")
    })
  }

  def isShownAtStrings(data: Document[NodeSeq]): Seq[String] =
    (data \ "identifier").map(node => getByAttribute(node, "type", "hdl"))
      .flatMap(extractStrings)
}
