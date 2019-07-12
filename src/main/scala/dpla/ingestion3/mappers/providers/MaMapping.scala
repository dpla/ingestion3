package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.enrichments.normalizations.filters.{DigitalSurrogateBlockList, FormatTypeValuesBlockList}
import dpla.ingestion3.mappers.utils.{Document, XmlExtractor, XmlMapping}
import dpla.ingestion3.messages.IngestMessageTemplates
import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model.{nameOnlyAgent, _}
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.xml._


class MaMapping extends XmlMapping with XmlExtractor with IngestMessageTemplates {

  val formatBlockList: Set[String] =
    DigitalSurrogateBlockList.termList ++
      FormatTypeValuesBlockList.termList

  // ID minting functions
  override def useProviderName(): Boolean = true

  override def getProviderName(): String = "bpl" // boston public library

  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] =
    extractString(data \ "header" \ "identifier")

  // SourceResource mapping
  override def alternateTitle(data: Document[NodeSeq]): ZeroToMany[String] =
  // <mods:titleInfo type="alternative"><mods:title>
  // <mods:titleInfo type="translated"><mods:title>
  // <mods:titleInfo type="uniform"><mods:title>
    (getModsRoot(data) \ "titleInfo")
      .map(node => getByAttribute(node, "type", "alternative"))
      .flatMap(altTitle => extractStrings(altTitle \ "title")) ++
    (getModsRoot(data) \ "titleInfo")
      .map(node => getByAttribute(node, "type", "translated"))
      .flatMap(altTitle => extractStrings(altTitle \ "title")) ++
    (getModsRoot(data) \ "titleInfo")
      .map(node => getByAttribute(node, "type", "uniform"))
      .flatMap(altTitle => extractStrings(altTitle \ "title"))

  override def collection(data: Document[NodeSeq]): Seq[DcmiTypeCollection] =
  // <relatedItem type="host"><titleInfo><title>
    (getModsRoot(data) \ "relatedItem")
      .flatMap(node => getByAttribute(node.asInstanceOf[Elem], "type", "host"))
      .flatMap(collection => extractStrings(collection \ "titleInfo" \ "title"))
      .map(nameOnlyCollection)

  // contributor
  // identify all names with a corresponding role (in @valueUri).
  // Is there a way to differentiate so that anyone with a “contributor”
  /**
  <mods:name>
    <mods:role>
      <mods:roleTerm authority="marcrelator" authorityURI="http://id.loc.gov/vocabulary/relators" type="text" valueURI="http://id.loc.gov/vocabulary/relators/ctb">Contributor</mods:roleTerm>
    </mods:role>
    <mods:namePart>American Photo-Relief Printing Co</mods:namePart>
  </mods:name>
  **/
  override def contributor(data: Document[NodeSeq]): ZeroToMany[EdmAgent] = {
    val names = (getModsRoot(data) \ "name")
      .filter(n => extractStrings(n \ "role" \ "roleTerm").map(_.trim).contains("Contributor"))

    nameConstructor(names)
  }

  override def creator(data: Document[NodeSeq]): Seq[EdmAgent] = {
  // <name><namePart> + " , " <namePart type=date>
    val names = (getModsRoot(data) \ "name")
      .filterNot(n => extractStrings(n \ "role" \ "roleTerm").map(_.trim).contains("Contributor"))

    nameConstructor(names)
  }

  private def nameConstructor(names: NodeSeq): Seq[EdmAgent] = {
    names.map(node => {
        val name = extractString((node \ "namePart").filter(p => p.attributes.isEmpty))
        val date = extractString((node \ "namePart").filter(p => filterAttribute(p, "type", "date")))

        if(date.isDefined)
          s"${name.getOrElse("")}, ${date.get}"
        else
          name.getOrElse("")
      })
      .map(nameOnlyAgent)
  }

  override def date(data: Document[NodeSeq]): Seq[EdmTimeSpan] = {
  // <mods:originInfo><mods:dateCreated encoding="w3cdtf" keyDate="yes">
  // <mods:originInfo><mods:dateIssued encoding="w3cdtf" keyDate="yes">
  // <mods:originInfo><mods:dateOther encoding="w3cdtf" keyDate="yes">
  // <mods:originInfo><mods:copyrightDate encoding="w3cdtf" keyDate="yes">
  // for date ranges @keyDate='yes' and @point='start' | @point='end'

    val props = Seq("dateCreated", "dateIssued", "dateOther", "copyrightDate")


    props.flatMap(prop => {
      val dateCreated = (getModsRoot(data) \ "originInfo" \ prop)
        .filterNot(node => filterAttribute(node,"point", "start") | filterAttribute(node,"point", "end"))
        .flatMap(node => getByAttribute(node, "keyDate", "yes"))
        .flatMap(node => getByAttribute(node, "encoding", "w3cdtf"))
        .flatMap(node => extractStrings(node))
        .map(_.trim)

      // Get dateCreated values with a keyDate=yes attribute and point=start attribute
      val earlyDate = (getModsRoot(data) \ "originInfo" \ prop)
        .flatMap(node => getByAttribute(node, "keyDate", "yes"))
        .flatMap(node => getByAttribute(node, "point", "start"))
        .flatMap(node => extractStrings(node))
        .map(_.trim)

      // Get dateCreated values with point=end attribute
      val lateDate = (getModsRoot(data) \ "originInfo" \ prop)
        .flatMap(node => getByAttribute(node, "point", "end"))
        .flatMap(node => extractStrings(node))
        .map(_.trim)

      (dateCreated.nonEmpty, earlyDate.nonEmpty, lateDate.nonEmpty) match {
        case (true, _, _) => dateCreated.map(stringOnlyTimeSpan)
        case (false, true, true) => Seq(EdmTimeSpan(
          originalSourceDate = Some(s"${earlyDate.headOption.getOrElse("")}-${lateDate.headOption.getOrElse("")}"),
          begin = earlyDate.headOption,
          end = lateDate.headOption
        ))
        case _ => Seq()
      }
    })
  }

  override def description(data: Document[NodeSeq]): Seq[String] =
  // <mods:abstract>
  // <mods:note>
    extractStrings(getModsRoot(data) \ "note") ++
      extractStrings(getModsRoot(data) \ "abstract")

  override def extent(data: Document[NodeSeq]): ZeroToMany[String] =
  // <mods:physicalDescription><mods:extent>
    extractStrings(getModsRoot(data) \ "physicalDescription" \ "extent")

  override def format(data: Document[NodeSeq]): Seq[String] =
  // <genre>
    extractStrings(getModsRoot(data) \ "genre")
      .map(_.applyBlockFilter(formatBlockList))
      .filter(_.nonEmpty)

  override def identifier(data: Document[NodeSeq]): Seq[String] =
  // <mods:identifier type="local-accession">
  // <mods:identifier type="local-other">
  // <mods:identifier type="local-call">
  // <mods:identifier type="local-barcode">
  // The value should be the type qualifier value + ": " + the property value, e.g., "Local accession: ####""
    {
      val types = Seq("local-accession", "local-other", "local-call", "local-barcode")

      types.map(t => {
        val label = t.split("-").mkString(" ").capitalize
        val id = (getModsRoot(data) \ "identifier")
          .flatMap(node => getByAttribute(node, "type", t))
          .flatMap(extractString(_))

        s"$label: $id"
      })
    }

  override def language(data: Document[NodeSeq]): Seq[SkosConcept] =
  // <mods:language><mods:languageTerm>
    extractStrings(getModsRoot(data) \ "language" \ "languageTerm")
      .map(nameOnlyConcept)

  override def place(data: Document[NodeSeq]): Seq[DplaPlace] =
  // <mods:subject><mods:hierarchicalGeographic> [combine all children into a single property with ', ' between them]
  // <mods:subject><mods:geographic>;
  // <mods:subject><mods:cartographics><mods:coordinates>
    {

      val subjects = getModsRoot(data) \ "subject"

      subjects.flatMap(subject => {
        val places = extractString(subject \ "geographic")
        val coordinates =  extractString(subject \ "cartographics" \ "coordinates")

        val hierarchy = (subject \ "hierarchicalGeographic").map(h => {
          val cityLabel = (extractString(h \ "city"), extractString(h \ "citySection")) match {
            case (Some(city), Some(citySection)) => Some(s"$city, $citySection")
            case (Some(city), None) => Some(city)
            case (None, Some(citySection)) => Some(citySection)
            case (_, _) => None
          }

          val country = extractString(h \ "country")
          val county = extractString(h \ "county")
          val state = extractString(h \ "state")
          val name = Option(Seq(cityLabel, county, state, country).flatten.map(_.trim).mkString(", "))

          DplaPlace(
            name = name,
            city = cityLabel,
            country = country,
            county = county,
            state = state,
            coordinates = coordinates
          )
        })

        if(places.isEmpty)
          hierarchy
        else
          Seq(DplaPlace(name = places,
            coordinates = coordinates))
      })
    }

  private def places(data: Document[NodeSeq]) =  extractStrings(getModsRoot(data) \ "subject" \ "geographic")

  override def publisher(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractStrings(getModsRoot(data) \ "originInfo" \ "publisher")
      .map(nameOnlyAgent)

  //  <accessCondition> when the @type="use and reproduction" attribute is not present
  override def rights(data: Document[NodeSeq]): AtLeastOne[String] =
    (getModsRoot(data) \ "accessCondition")
      .flatMap(extractStrings)

  override def subject(data: Document[NodeSeq]): Seq[SkosConcept] =
  // Any <mods:subject> field, except <mods:hierarchicalGeographic>, <mods:geographic>, and <mods:cartographics><mods:coordinates>.
    ( (getModsRoot(data) \ "subject" \ "topic") ++
      (getModsRoot(data) \ "subject" \ "temporal") ++
      (getModsRoot(data) \ "subject" \ "titleInfo") ++
      (getModsRoot(data) \ "subject" \ "name") ++
      (getModsRoot(data) \ "subject" \ "genre"))
      .flatMap(extractStrings)
      .map(_.trim)
      .map(nameOnlyConcept)

  override def temporal(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
    extractStrings(getModsRoot(data) \ "subject" \\ "temporal").map(stringOnlyTimeSpan)

  override def title(data: Document[NodeSeq]): Seq[String] =
  // <titleInfo usage="primary"> children combined as follows (with a single space between):
  // <nonSort> <title> <subTitle> <partName> <partNumber>
  {
    val titleNodes = (getModsRoot(data) \ "titleInfo")
      .flatMap(node => getByAttribute(node, "usage", "primary"))

    val nonSort = extractStrings(titleNodes \ "nonSort").mkString(" ")
    val title = extractStrings(titleNodes \ "title").mkString(" ")
    val subTitle = extractStrings(titleNodes \ "subTitle").mkString(" ")
    val partName = extractStrings(titleNodes \ "partName").mkString(" ")
    val partNumber = extractStrings(titleNodes \ "partNumber").mkString(" ")

    Seq(s"$nonSort $title $subTitle $partName $partNumber".reduceWhitespace)
  }

  override def `type`(data: Document[NodeSeq]): Seq[String] =
  // <mods:typeOfResource>
    extractStrings(getModsRoot(data) \ "typeOfResource")

  // OreAggregation
  // TODO IIIF <mods:location><mods:url note="iiif-manifest">

  override def dplaUri(data: Document[NodeSeq]): ZeroToOne[URI] = mintDplaItemUri(data)

  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
  // <mods:location><mods:physicalLocation>
    (getModsRoot(data) \ "location" \ "physicalLocation")
      .flatMap(extractStrings)
      .map(nameOnlyAgent)

  override def edmRights(data: Document[NodeSeq]): ZeroToMany[URI] =
    (getModsRoot(data) \ "accessCondition")
      .flatMap(node => getByAttribute(node.asInstanceOf[Elem], "type", "use and reproduction"))
      .flatMap(node => node.attribute(node.getNamespace("xlink"), "href"))
      .flatMap(n => extractString(n.head))
      .map(URI)

  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
  // <mods:location><mods:url usage="primary" access="object in context">
    (getModsRoot(data) \ "location" \ "url")
      .flatMap(node => getByAttribute(node, "usage", "primary"))
      .flatMap(node => getByAttribute(node, "access", "object in context"))
      .flatMap(extractStrings)
      .map(stringOnlyWebResource)

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] = Utils.formatXml(data)

  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
  // <mods:location><mods:url access="preview">
    (getModsRoot(data) \ "location" \ "url")
      .flatMap(node => getByAttribute(node.asInstanceOf[Elem], "access", "preview"))
      .flatMap(extractStrings)
      .map(stringOnlyWebResource)

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = agent

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(data))

  // Helper method
  def agent = EdmAgent(
    name = Some("Digital Commonwealth"),
    uri = Some(URI("http://dp.la/api/contributor/digital-commonwealth"))
  )

  private lazy val getModsRoot = (data: Document[NodeSeq]) => data \ "metadata" \ "mods"
}

