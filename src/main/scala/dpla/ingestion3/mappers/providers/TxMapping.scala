package dpla.ingestion3.mappers.providers

import java.net.URL

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.enrichments.normalizations.filters.{DigitalSurrogateBlockList, FormatTypeValuesBlockList}
import dpla.ingestion3.mappers.utils.{Document, XmlExtractor, XmlMapping}
import dpla.ingestion3.messages.IngestMessageTemplates
import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model.{EdmAgent, EdmTimeSpan, EdmWebResource, URI, _}
import dpla.ingestion3.utils.{HttpUtils, Utils}
import org.json4s.JsonAST
import org.json4s.JsonDSL._

import scala.collection.mutable.ArrayBuffer
import scala.xml.NodeSeq

class TxMapping extends XmlMapping with XmlExtractor with IngestMessageTemplates {
  val formatBlockList: Set[String] =
    DigitalSurrogateBlockList.termList ++
      FormatTypeValuesBlockList.termList

  override def getProviderName: String = "texas"

  override def dplaUri(data: Document[NodeSeq]): ZeroToOne[URI] = mintDplaItemUri(data)

  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] = {
    val dataProviders = extractStrings(data \ "header" \ "setSpec")
      .filter(_.startsWith("partner"))
      .map(setSpec => TxMapping.dataproviderTermLabel.getOrElse(setSpec.split(":").last, ""))
      .filter(_.nonEmpty)

    dataProviders.lastOption match {
      case Some(dataProvider) => Seq(nameOnlyAgent(dataProvider))
      case None => Seq()
    }
  }

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] = Utils.formatXml(data)

  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    (metadata(data) \ "identifier")
      .filter(node => filterAttribute(node, "qualifier", "itemURL"))
      .flatMap(extractStrings)
      .map(stringOnlyWebResource)

  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    (metadata(data) \ "identifier")
      .filter(node => filterAttribute(node, "qualifier", "thumbnailURL"))
      .flatMap(extractStrings)
      .map(stringOnlyWebResource)

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] =
    EdmAgent(
      name = Some("The Portal to Texas History"),
      uri = Some(URI("http://dp.la/api/contributor/the_portal_to_texas_history"))
    )

  override def sidecar(data: Document[NodeSeq]): JsonAST.JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(data))

  override def useProviderName: Boolean = true

  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] =
    extractString(data \ "header" \ "identifier")
      .map(_.trim)

  // dpla.sourceResource
  override def contributor(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    extractName(metadata(data), "contributor")

  override def creator(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    extractName(metadata(data), "creator")

  override def date(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] = {
    val creationDates = (data \ "metadata" \ "date")
      .filter(node => filterAttribute(node, "qualifier", "creation"))
      .flatMap(extractStrings)
      .map(stringOnlyTimeSpan)
      .headOption

    val otherDates = (data \ "metadata" \ "date")
      .filterNot(node => filterAttribute(node, "qualifier", "digitized") | filterAttribute(node, "qualifier", "embargoUntil"))
      .flatMap(extractStrings)
      .map(stringOnlyTimeSpan)
      .headOption

    // Return only the first instance of either creation date or any other valid date
    (creationDates ++ otherDates).headOption.toSeq
  }

  override def description(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(metadata(data) \ "description")

  override def format(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(metadata(data) \ "resourceType")
      .map(_.splitAtDelimiter("_").head)
      .map(_.applyBlockFilter(formatBlockList))
      .filter(_.nonEmpty)

  override def identifier(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(metadata(data) \ "identifier")

  override def language(data: Document[NodeSeq]): ZeroToMany[SkosConcept] =
    extractStrings(metadata(data) \ "language")
      .map(nameOnlyConcept)

  override def place(data: Document[NodeSeq]): ZeroToMany[DplaPlace] = {
    val qualifiers = Seq("placeName", "placePoint", "placeBox")

    (metadata(data) \ "coverage")
      .filter(node => filterAttributeListOptions(node, "qualifier", qualifiers))
      .flatMap(extractStrings)
      .map(nameOnlyPlace)
  }

  override def publisher(data: Document[NodeSeq]): ZeroToMany[EdmAgent] = {
    // Only create a publisher value when <publisher> containers both <location> and <name> sub-properties
    //    <untl:publisher>
    //      <untl:location>Philadelphia</untl:location>
    //      <untl:name>Charles Desilver</untl:name>
    //    </untl:publisher>

    //    For the above example, the expected mapped publisher value is: 'Philadelphia: Charles Desilver'

    val locations = extractStrings(metadata(data) \ "publisher" \ "location")
    val names = extractStrings(metadata(data) \ "publisher" \ "name")

    locations.zipAll(names, None, None).flatMap {
      case (location: String, name: String) => Some(s"$location: $name")
      case (_, _) => None
    }.map(nameOnlyAgent)
  }

  override def relation(data: Document[NodeSeq]): ZeroToMany[LiteralOrUri] =
    extractStrings(data \ "metadata" \ "relation").map(eitherStringOrUri)

  override def rights(data: Document[NodeSeq]): AtLeastOne[String] =
    extractStrings(metadata(data) \ "rights")
      .map(text => TxMapping.rightsTermLabel.getOrElse(text, text))

  override def subject(data: Document[NodeSeq]): ZeroToMany[SkosConcept] =
    extractStrings(metadata(data) \ "subject")
      .map(nameOnlyConcept)

  override def title(data: Document[NodeSeq]): AtLeastOne[String] =
    extractStrings(metadata(data) \ "title")

  override def `type`(data: Document[NodeSeq]): AtLeastOne[String] = {
    // This greatly simplifies the ingestion1 mapping and pushes the filtering logic from ingestion1 to the ingestion3
    // type enrichment
    extractStrings(metadata(data) \ "format")
  }

  /**
    * Helper method to extract value directly associated with property or <name> sub-property
    *
    * @param data
    * @param property
    * @return
    */
  def extractName(data: NodeSeq, property: String): ZeroToMany[EdmAgent] = {
    (data \ property)
      .flatMap(node => {
        val name = extractStrings(node \ "name")
        val propertyValue = node.child match {
          case _: ArrayBuffer[String] => Seq(node.text)
          case _ => Seq()
        }

        // If propertyValue is empty then return the value of the <name> sub-property
        if(propertyValue.isEmpty)
          name
        else
          propertyValue
      })
      .map(nameOnlyAgent)
  }

  /**
    * Helper method to get to metadata root
    *
    * @param data
    * @return
    */
  def metadata(data: NodeSeq): NodeSeq = data \ "metadata" \ "metadata"
}


object TxMapping {
  import org.json4s.JsonAST._

  val rightsTermLabel: Map[String, String] = Map[String, String](
    "by" -> "License: Attribution.",
    "by-nc"-> "License: Attribution Noncommercial.",
    "by-nc-nd"-> "License: Attribution Non-commercial No Derivatives.",
    "by-nc-sa"-> "License: Attribution Noncommercial Share Alike.",
    "by-nd"-> "License: Attribution No Derivatives.",
    "by-sa"-> "License: Attribution Share Alike.",
    "copyright"-> "License: Copyright.",
    "pd"-> "License: Public Domain."
  )

  val endpoint = "https://digital2.library.unt.edu/vocabularies/institutions/json/"
  val jsonString = HttpUtils.makeGetRequest(new URL(endpoint), None).getOrElse("")
  val json = org.json4s.jackson.JsonMethods.parse(jsonString)

  val dataproviderTermLabel: Map[String, String] = (for {
    JArray(terms) <- json \ "terms"
    JObject(term) <- terms
    JField("name", JString(name)) <- term
    JField("label", JString(label)) <- term
  } yield (name -> label)).toMap[String, String]

}