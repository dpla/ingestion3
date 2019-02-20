package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.filters.{DigitalSurrogateBlockList, FormatTypeValuesBlockList}
import dpla.ingestion3.mappers.utils.{Document, XmlExtractor, XmlMapping}
import dpla.ingestion3.messages.IngestMessageTemplates
import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model.{nameOnlyAgent, _}
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.xml._


class MichiganMapping extends XmlMapping with XmlExtractor with IngestMessageTemplates {

  val formatBlockList: Set[String] =
    DigitalSurrogateBlockList.termList ++
      FormatTypeValuesBlockList.termList

  // ID minting functions
  override def useProviderName(): Boolean = true

  override def getProviderName(): String = "mi"

  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] =
    extractString(data \ "header" \ "identifier")
      .map(_.trim)

  // SourceResource mapping
  override def collection(data: Document[NodeSeq]): ZeroToMany[DcmiTypeCollection] =
  // <mods:relatedItem type=host><mods:titleInfo><mods:title>
    (data \\ "relatedItem")
      .flatMap(node => getByAttribute(node.asInstanceOf[Elem], "type", "host"))
      .flatMap(collection => extractStrings(collection \ "titleInfo" \ "title"))
      .map(nameOnlyCollection)

  override def contributor(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
  // when <role><roleTerm> DOES equal "contributor>
    (data \\ "name")
      .filter(node => (node \ "role" \ "roleTerm").text.equalsIgnoreCase("contributor"))
      .flatMap(n => extractStrings(n \ "namePart"))
      .map(nameOnlyAgent)

  override def creator(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
  // <mods:name><mods:namePart> when <role><roleTerm> is 'creator' or blank
    (data \\ "name")
      .filter(node => {
        val role = (node \ "role" \ "roleTerm").text
        role.isEmpty || role.equalsIgnoreCase("creator")
      } )
      .flatMap(n => extractStrings(n \ "namePart"))
      .map(nameOnlyAgent)

  override def date(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] = {
    // <mods:originInfo><mods:dateCreated>
    val dateCreated = extractStrings(data \\ "originInfo" \\ "dateCreated")
      .map(stringOnlyTimeSpan)

    // Get dateIssued values
    val dateIssued = (data \\ "originInfo" \\ "dateIssued")
      .filter(node => node.attributes.get("point").isEmpty)
      .flatMap(node => extractStrings(node))
      .map(stringOnlyTimeSpan)
    // Get dateIssued values with attribute of point=start
    val dateIssuedEarly = (data \\ "originInfo" \\ "dateIssued")
      .flatMap(node => getByAttribute(node.asInstanceOf[Elem], "point", "start"))
      .flatMap(node => extractStrings(node))
    // Get dateIssued values with attribute of point=end
    val dateIssuedLate = (data \\ "originInfo" \\ "dateIssued")
      .flatMap(node => getByAttribute(node.asInstanceOf[Elem], "point", "end"))
      .flatMap(node => extractStrings(node))

    val constructedDateIssued = if(dateIssuedEarly.length == dateIssuedLate.length) {
        dateIssuedEarly.zip(dateIssuedLate).map {
          case (begin: String, end: String) =>
            EdmTimeSpan(
              originalSourceDate = Some(s"$begin-$end"),
              begin = Some(begin),
              end = Some(end)
            )
        }
    } else {
      Seq()
    }

    (dateCreated.nonEmpty, dateIssued.nonEmpty, constructedDateIssued.nonEmpty) match {
      case (true, _, _) => dateCreated // if any dateCreated exist return only those values
      case (false, true, _) => dateIssued // if any dateIssued exist return only those values
      case (false, false, true) => constructedDateIssued // if neither dateCreated or dateIssued exist return constructed
      case _ => Seq()
    }
  }

  override def description(data: Document[NodeSeq]): Seq[String] = {
    // <mods:note> and <mods:abstract> and <mods:physicalDescription> \ <note>
    extractStrings(data \\ "physicalDescription" \ "note") ++
      extractStrings(data \\ "abstract") ++
      extractStrings(data \ "note")
  }

  override def extent(data: Document[NodeSeq]): ZeroToMany[String] =
  // <mods:physicalDescription><mods:extent>
    extractStrings(data \\ "physicalDescription" \ "extent")

  override def format(data: Document[NodeSeq]): Seq[String] =
  // <mods:genre> AND <mods:physicialDescription><mods:form>
    extractStrings(data \\ "genre") ++
      extractStrings(data \\ "physicalDescription" \ "form")

  override def identifier(data: Document[NodeSeq]): Seq[String] =
  // <mods:identifier>
    extractStrings(data \ "metadata" \ "mods" \ "identifier")

  override def language(data: Document[NodeSeq]): Seq[SkosConcept] =
  // <mods:language><mods:languageTerm>
    extractStrings(data \\ "language" \\ "languageTerm")
      .map(nameOnlyConcept)

  override def place(data: Document[NodeSeq]): Seq[DplaPlace] =
  // <mods:subject><mods:geographic>
    extractStrings(data \\ "subject" \\ "geographic")
      .map(nameOnlyPlace)

  override def publisher(data: Document[NodeSeq]): Seq[EdmAgent] =
  // <mods:originInfo><mods:publisher>
    extractStrings(data \\ "originInfo" \\ "publisher")
      .map(nameOnlyAgent)

  //  <accessCondition> when the @type="use and reproduction" attribute is not present
  override def rights(data: Document[NodeSeq]): AtLeastOne[String] =
    extractStrings(data \ "metadata" \ "mods" \ "accessCondition")

  override def subject(data: Document[NodeSeq]): Seq[SkosConcept] =
  // <mods:subject> AND
  // <mods:subject><mods:topic> AND
  // <mods:subject><mods:name><mods:namePart> AND
  // <mods:subject><mods:genre> AND
  // <mods:subject><mods:titleInfo><mods:title>
    (extractStrings(data \\ "subject" \ "topic") ++
      extractStrings(data \\ "subject" \ "name" \ "namePart") ++
      extractStrings(data \\ "subject" \ "genre") ++
      extractStrings(data \\ "subject" \ "titleInfo" \ "title") ++
      extractStrings(data \\ "subject")
    ).map(nameOnlyConcept)

  override def temporal(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
  // <mods:subject><mods:temporal>
    extractStrings(data \\ "subject" \ "temporal")
      .map(stringOnlyTimeSpan)

  override def title(data: Document[NodeSeq]): Seq[String] =
  // <mods:titleInfo><mods:title>
    extractStrings(data \\ "mods" \ "titleInfo" \ "title")

  override def `type`(data: Document[NodeSeq]): Seq[String] =
  // <mods:typeofresource>
    extractStrings(data \ "metadata" \ "mods" \ "typeOfResource")

  // OreAggregation
  override def dplaUri(data: Document[NodeSeq]): ZeroToOne[URI] = mintDplaItemUri(data)

  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
  // first <mods:recordInfo><mods:recordContentSource>
    extractStrings(data \\ "recordInfo" \ "recordContentSource")
      .map(nameOnlyAgent)
      .take(1)

  override def intermediateProvider(data: Document[NodeSeq]): ZeroToOne[EdmAgent] = {
    // second <mods:recordInfo><mods:recordContentSource> if exists
    val providers = extractStrings(data \\ "recordInfo" \ "recordContentSource")
      .map(nameOnlyAgent)
    if (providers.length > 2)
      Some(providers(1))
    else None
  }

  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
  // <mods:location><mods:url usage="primary display" access="object in  context">
    (data \ "metadata" \ "mods" \ "location" \ "url")
      .flatMap(node => getByAttribute(node.asInstanceOf[Elem], "usage", "primary"))
      .flatMap(extractStrings)
      .map(stringOnlyWebResource)

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] = Utils.formatXml(data)

  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
  // <mods:location><mods:url access="preview">
    (data \ "metadata" \ "mods" \ "location" \ "url")
      .flatMap(node => getByAttribute(node.asInstanceOf[Elem], "access", "preview"))
      .flatMap(extractStrings)
      .map(stringOnlyWebResource)

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = agent

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(data))

  // Helper method
  def agent = EdmAgent(
    name = Some("Michigan Service Hub"),
    uri = Some(URI("http://dp.la/api/contributor/michigan"))
  )
}

