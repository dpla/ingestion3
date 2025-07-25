package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.mappers.utils._
import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.json4s.JsonAST
import org.json4s.JsonDSL._

import scala.xml.{NodeSeq, Text}

class SiMapping extends XmlMapping with XmlExtractor {

  // allow records without rights
  override val enforceRights: Boolean = false

  // ID minting functions
  override def useProviderName: Boolean = true

  override def getProviderName: Option[String] = Some("smithsonian")

  override def originalId(implicit
      data: Document[NodeSeq]
  ): ZeroToOne[String] =
    // Hard code URL query for item as basis for DPLA identifier to maintain SI ids between ingestion1 and ingestion3
    // The URL is unnecessary and DPLA should ideally only rely upon the record_ID value. This does not exactly match
    // the itemUri value because it uses %3A instead of `=` in one place (again for consistency between ingestion1 and
    // ingestion3.
    Some(
      s"http://collections.si.edu/search/results.htm?" +
        s"q=record_ID%%3A${getRecordId(data).getOrElse(throw MappingException("Missing required property `record_ID`"))}" +
        s"&repo=DPLA"
    )

  // OreAggregation
  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    extractStrings(data \ "descriptiveNonRepeating" \ "data_source")
      .map(nameOnlyAgent)

  override def dplaUri(data: Document[NodeSeq]): ZeroToOne[URI] =
    mintDplaItemUri(data)

  override def edmRights(data: Document[NodeSeq]): ZeroToMany[URI] = {
    val objectRights = extractStrings(data \\ "objectRights")
      .filter(_.nonEmpty)
      .flatMap(lookupRightsUriFromText)
    val mediaRights = rightsFromMedia(
      extractStrings(data \\ "media" \ "usage" \ "access")
    )
    if (objectRights.nonEmpty)
      objectRights
    else if (mediaRights.nonEmpty)
      mediaRights.toSeq
    else Seq()
  }

  private def lookupRightsUriFromText(str: String): Option[URI] =
    str.toLowerCase.trim match {
      case "cc0" =>
        Some(URI("http://creativecommons.org/publicdomain/zero/1.0/"))
      case "in copyright" =>
        Some(URI("http://rightsstatements.org/vocab/InC/1.0/"))
      case "in copyright - eu orphan work" =>
        Some(URI("http://rightsstatements.org/vocab/InC-EDU/1.0/"))
      case "in copyright - non-commercial use permitted" =>
        Some(URI("http://rightsstatements.org/vocab/InC-EDU/1.0/"))
      case "in copyright - educational use permitted" =>
        Some(URI("http://rightsstatements.org/vocab/InC-NC/1.0/"))
      case "in copyright - rights- holder(s) unlocatable or unidentifiable" =>
        Some(URI("http://rightsstatements.org/vocab/InC-RUU/1.0/"))
      case "no copyright - contractual restrictions" =>
        Some(URI("http://rightsstatements.org/vocab/NoC-CR/1.0/"))
      case "no copyright - non-commercial use only" =>
        Some(URI("http://rightsstatements.org/vocab/NoC-NC/1.0/"))
      case "no copyright - other known legal restrictions" =>
        Some(URI("http://rightsstatements.org/vocab/NoC-OKLR/1.0/"))
      case "no copyright - united states" =>
        Some(URI("http://rightsstatements.org/vocab/NoC-US/1.0/"))
      case "copyright not evaluated" =>
        Some(URI("http://rightsstatements.org/vocab/CNE/1.0/"))
      case "copyright undetermined" =>
        Some(URI("http://rightsstatements.org/vocab/UND/1.0/"))
      case "no known copyright" =>
        Some(URI("http://rightsstatements.org/vocab/NKC/1.0/"))
      case _ => None
    }

  private def rightsFromMedia(strs: Seq[String]): ZeroToOne[URI] = {
    val cleaned = strs.map(_.trim.toLowerCase).filter(_.nonEmpty)
    if (cleaned.contains("usage conditions apply"))
      Some(URI("http://rightsstatements.org/vocab/InC/1.0/"))
    else if (cleaned.contains("not determined"))
      Some(URI("http://rightsstatements.org/vocab/CNE/1.0/"))
    else if (cleaned.contains("cc0"))
      Some(URI("http://creativecommons.org/publicdomain/zero/1.0/"))
    else
      None
  }

  override def mediaMaster(
      data: Document[NodeSeq]
  ): ZeroToMany[EdmWebResource] =
    (data \\ "online_media" \ "media")
      .flatMap(_.child)
      .flatMap(_ match {
        case Text(str) if str.trim.nonEmpty => Some(str)
        case _                              => None
      })
      .map(stringOnlyWebResource)

  override def isShownAt(
      data: Document[NodeSeq]
  ): ZeroToMany[EdmWebResource] = {

    val guidDerived = for {
      guid <- data \ "descriptiveNonRepeating" \ "guid"
      textValue = guid.text
    } yield uriOnlyWebResource(URI(textValue))

    if (guidDerived.nonEmpty) guidDerived
    else
      getRecordId(data)
        .map(id =>
          uriOnlyWebResource(
            URI(
              s"http://collections.si.edu/search/results.htm?" +
                s"q=record_ID=$id" + s"&repo=DPLA"
            )
          )
        )
        .toSeq
  }

  override def `object`(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] = {
    val results = (data \ "descriptiveNonRepeating" \ "online_media" \ "media")
      .flatMap(_.child)
      .flatMap(_ match {
        case Text(str) if str.trim.nonEmpty => Some(str)
        case _                              => None
      })
      .map(stringOnlyWebResource)

    results match {
      case Seq()              => Seq()
      case x if x.length == 1 => results
      case x if x.length > 1  =>
        // if there's more than one, prefer one from ids.si.edu
        x.find(entry => entry.uri.toString.contains("ids.si.edu")) match {
          case Some(entry) => Seq(entry)
          case None        => Seq(results.head)
        }
    }
  }

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] =
    Utils.formatXml(data)

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] =
    EdmAgent(
      name = Some("Smithsonian Institution"),
      uri = Some(URI("http://dp.la/api/contributor/smithsonian"))
    )

  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    (data \ "descriptiveNonRepeating" \ "online_media" \ "media")
      .flatMap(node => node.attribute("thumbnail"))
      .flatMap(node => extractStrings(node))
      .map(stringOnlyWebResource)
      .headOption
      .toSeq

  override def sidecar(data: Document[NodeSeq]): JsonAST.JValue =
    ("prehashId", buildProviderBaseId()(data)) ~ ("dplaId", mintDplaId(data))

  // SourceResource
  override def collection(data: Document[NodeSeq]): Seq[DcmiTypeCollection] =
    extractStrings(data \ "freetext" \ "setName").map(nameOnlyCollection)

  override def contributor(data: Document[NodeSeq]): Seq[EdmAgent] =
    (data \ "freetext" \ "name")
      .filter(node => filterAttribute(node, "label", "contributor"))
      .flatMap(extractStrings)
      .map(nameOnlyAgent)

  override def creator(data: Document[NodeSeq]): Seq[EdmAgent] =
    // might need to add this extensive set of label={value} filters to the mapping:
    //    val creatorAttr = Seq("Architect", "Artist", "Artists/Makers", "Attributed to", "Author", "Cabinet Maker",
    //      "Ceramist", "Circle of", "Co-Designer", "Creator", "Decorator", "Designer", "Draftsman", "Editor", "Embroiderer",
    //      "Engraver", "Etcher", "Executor", "Follower of", "Graphic Designer", "Instrumentiste", "Inventor",
    //      "Landscape Architect", "Landscape Designer", "Maker", "Model Maker/maker", "Modeler", "Painter", "Photographer",
    //      "Possible attribution", "Possibly", "Possibly by", "Print Maker", "Printmaker", "Probably", "School of", "Sculptor",
    //      "Studio of", "Workshop of", "Weaver", "Writer", "animator", "architect", "artist", "artist.", "artist?",
    //      "artist attribution", "author", "author.", "author?", "authors?", "caricaturist", "cinematographer", "composer",
    //      "composer, lyricist", "composer; lyrcist", "composer; lyricist", "composer; performer", "composer; recording artist",
    //      "composer?", "creator", "creators", "designer", "developer", "director", "editor", "engraver", "ethnographer", "fabricator",
    //      "filmmaker", "filmmaker, anthropologist", "garden designer", "graphic artist", "illustrator", "inventor",
    //      "landscape Architect", "landscape architect", "landscape architect, photographer", "landscape designer",
    //      "lantern slide maker", "lithographer", "lyicist", "lyicrist", "lyricist", "lyricist; composer", "maker", "maker (possibly)",
    //      "maker or owner", "maker; inventor", "original artist", "performer", "performer; composer; lyricist",
    //      "performer; recording artist", "performers", "performing artist; recipient", "performing artist; user", "photgrapher",
    //      "photograher", "photographer", "photographer and copyright claimant", "photographer and/or colorist", "photographer or collector",
    //      "photographer?", "photographerl", "photographerphotographer", "photographers", "photographers?", "photographer}",
    //      "photographic firm", "photogrpaher", "playwright", "poet", "possible maker", "printer", "printmaker", "producer",
    //      "recordig artist", "recording artist", "recording artist; composer", "recordist", "recordng artist", "sculptor",
    //      "shipbuilder", "shipbuilders", "shipping firm", "weaver", "weaver or owner")
    (data \ "freetext" \ "name")
      .filterNot(node => filterAttribute(node, "label", "contributor"))
      .flatMap(extractStrings)
      .map(
        nameOnlyAgent
      )

  override def date(data: Document[NodeSeq]): Seq[EdmTimeSpan] = extractDate(
    data
  )

  override def description(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(data \ "freetext" \ "notes")

  override def extent(data: Document[NodeSeq]): ZeroToMany[String] =
    (data \ "freetext" \ "physicalDescription")
      .filter(node => filterAttribute(node, "label", "Dimensions"))
      .flatMap(extractStrings)

  override def format(data: Document[NodeSeq]): ZeroToMany[String] =
    (data \ "freetext" \ "physicalDescription")
      .filter(node => {
        filterAttribute(
          node,
          "label",
          "Physical description"
        ) || filterAttribute(node, "label", "Medium")
      })
      .flatMap(extractStrings)

  override def identifier(data: Document[NodeSeq]): ZeroToMany[String] =
    for {
      identifierProp <- data \ "freetext" \ "identifier"
      identifier <- extractStrings(identifierProp)
      attrValue <- identifierProp.attribute("label").flatMap(extractString(_))
      if attrValue.startsWith("Catalog") || attrValue.startsWith("Accession")
    } yield identifier

  override def language(data: Document[NodeSeq]): ZeroToMany[SkosConcept] =
    extractStrings(data \\ "indexedStructured" \ "language")
      .map(_.replaceAll(" language", "")) // removes ' language' from term
      .map(nameOnlyConcept)

  override def place(data: Document[NodeSeq]): ZeroToMany[DplaPlace] = {
    // Get values from <indexedStructured> \ <geoLocation>

    //  <geoLocation><L2 type=[Country | Nation]></geoLocation> MAPS TO DplaPlace.country
    //  <geoLocation><L3 type=[State | Province]></geoLocation> MAPS TO DplaPlace.state
    //  <geoLocation><L4 type=[County | Island]></geoLocation>  MAPS TO DplaPlace.county
    //  <geoLocation><Other></geoLocation>                      MAPS TO DplaPlace.region
    //  <geoLocation><L5 type=[City | Town]></geoLocation >     MAPS TO DplaPlace.city

    // <geoLocation>
    //  <points label=[text]>
    //    <point>
    //      <latitude type=[decimal OR degrees]>
    //      <longitude type=[decimal OR degrees]>
    //    </point>
    //  </points>
    // </geoLocation>
    //
    // Join lat and long values with a comma                    MAPS TO DplaPlace.coordinates

    // if those do not exist then pull values from freetext \ place

    def valueToOption(str: String): Option[String] = str match {
      case "" => None
      case _  => Some(str)
    }

    val preciseGeoLocation =
      (data \ "indexedStructured" \ "geoLocation").map(node => {

        val country = (node \ "L2")
          .flatMap(n =>
            getByAttribute(n, "type", "Country") ++ getByAttribute(
              n,
              "type",
              "Nation"
            )
          )
          .flatMap(extractStrings(_))
          .mkString(", ")

        val state = (node \ "L3")
          .flatMap(n =>
            getByAttribute(n, "type", "State") ++ getByAttribute(
              n,
              "type",
              "Province"
            )
          )
          .flatMap(extractStrings(_))
          .mkString(", ")

        val county = (node \ "L4")
          .flatMap(n =>
            getByAttribute(n, "type", "County") ++ getByAttribute(
              n,
              "type",
              "Island"
            )
          )
          .flatMap(extractStrings(_))
          .mkString(", ")

        val city = (node \ "L5")
          .flatMap(n =>
            getByAttribute(n, "type", "City") ++ getByAttribute(
              n,
              "type",
              "Town"
            )
          )
          .flatMap(extractStrings(_))
          .mkString(", ")

        val region = extractStrings(node \ "Other")
          .mkString(", ")

        val lat = (node \ "points" \ "point" \ "latitude")
          .filter(node =>
            filterAttribute(node, "type", "decimal") | filterAttribute(
              node,
              "type",
              "degrees"
            )
          )
          .flatMap(extractStrings(_))

        val long = (node \ "points" \ "point" \ "longitude")
          .filter(node =>
            filterAttribute(node, "type", "decimal") | filterAttribute(
              node,
              "type",
              "degrees"
            )
          )
          .flatMap(extractStrings(_))

        val point =
          lat.zipAll(long, None, None).map(p => s"${p._1},${p._2}").mkString

        DplaPlace(
          country = valueToOption(country),
          state = valueToOption(state),
          county = valueToOption(county),
          region = valueToOption(region),
          city = valueToOption(city),
          coordinates = valueToOption(point)
        )
      })

    // return structured DPLA Place if non-empty, otherwise use freetext spatial information
    if (preciseGeoLocation.nonEmpty) preciseGeoLocation
    else
      (data \ "freetext" \ "place")
        .flatMap(extractStrings)
        .map(nameOnlyPlace)
  }

  override def publisher(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    (data \ "freetext" \ "publisher")
      .filter(node => filterAttribute(node, "label", "publisher"))
      .flatMap(extractStrings)
      .map(nameOnlyAgent)

  override def rights(data: Document[NodeSeq]): AtLeastOne[String] = {
    val mediaRights =
      (data \ "descriptiveNonRepeating" \ "online_media" \ "media")
        .flatMap(node => node.attribute("rights"))
        .flatMap(extractStrings(_))

    if (mediaRights.isEmpty)
      (data \ "freetext" \ "creditLine")
        .filter(node => filterAttribute(node, "label", "credit line"))
        .flatMap(extractStrings(_)) ++
        (data \ "freetext" \ "objectRights")
          .filter(node => filterAttribute(node, "label", "rights"))
          .flatMap(extractStrings(_))
    else
      mediaRights
  }

  override def subject(data: Document[NodeSeq]): ZeroToMany[SkosConcept] = {
    val subjectProps = Seq(
      "topic",
      "name",
      "culture",
      "tax_kingdom",
      "tax_phylum",
      "tax_division",
      "tax_class",
      "tax_order",
      "tax_family",
      "tax_sub-family",
      "scientific_name",
      "common_name",
      "strat_group",
      "strat_formation",
      "strat_member"
    )
    val topicAttrLabels = Seq("Topic", "subject", "event")

    (subjectProps.flatMap(subjectProp =>
      extractStrings(data \ "indexedStructured" \ subjectProp)
    ) ++
      topicAttrLabels.flatMap(topic => {
        (data \ "freetext" \ "topic")
          .flatMap(node => getByAttribute(node, "label", topic))
          .flatMap(extractStrings(_))
      }))
      .flatMap(_.splitAtDelimiter("\\\\"))
      .flatMap(_.splitAtDelimiter(":"))
      .map(nameOnlyConcept)
  }

  override def temporal(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
    extractDate(data)

  override def title(data: Document[NodeSeq]): AtLeastOne[String] =
    (data \ "descriptiveNonRepeating" \ "title")
      .filter(node =>
        filterAttribute(node, "label", "title") ||
          filterAttribute(node, "label", "object name") ||
          filterAttribute(node, "label", "title (spanish)")
      )
      .flatMap(node => extractStrings(node))

  override def `type`(data: Document[NodeSeq]): ZeroToMany[String] =
    (data \ "freetext" \ "objectType")
      .flatMap(node => getByAttribute(node, "label", "Type"))
      .flatMap(extractStrings(_)) ++
      extractStrings(data \ "freetext" \ "physicalDescription") ++
      extractStrings(data \ "indexedStructure" \ "object_type")

  // Helper methods
  private def extractDate(data: Document[NodeSeq]): Seq[EdmTimeSpan] =
    (data \ "freetext" \ "date")
      .filter(node => node.attributes.get("label").nonEmpty)
      .flatMap(extractStrings(_))
      .map(stringOnlyTimeSpan) // done

  private def getRecordId(implicit data: Document[NodeSeq]): ZeroToOne[String] =
    extractString(data \ "descriptiveNonRepeating" \ "record_ID")
}
