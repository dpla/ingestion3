package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.mappers.utils._
import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.json4s.JsonAST
import org.json4s.JsonDSL._

import scala.xml.NodeSeq


class SiMapping extends XmlMapping with XmlExtractor {

  // ID minting functions
  override def useProviderName(): Boolean = true

  override def getProviderName(): String = "si" // FIXME confirm prefix

  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] =
    extractString(data \ "descriptiveNonRepeating" \ "record_ID")

  def itemUri(implicit data: Document[NodeSeq]): URI =
    URI(s"http://collections.si.edu/search/results.htm?" +
      s"q=record_ID=${originalId(data).getOrElse(throw MappingException("Missing required property `recordId`"))}" +
      s"&repo=DPLA")

  // OreAggregation
  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    extractStrings(data \ "descriptiveNonRepeating" \ "data_source")
      .map(nameOnlyAgent) // FIXME Take only first one?

  override def dplaUri(data: Document[NodeSeq]): ZeroToOne[URI] = mintDplaItemUri(data)

  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    Seq(uriOnlyWebResource(itemUri(data))) // done

  override def `object`(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    extractPreview(data) // done

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] =
    Utils.formatXml(data) // done

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] =
    agent // done

  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    extractPreview(data) // done

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

  override def creator(data: Document[NodeSeq]): Seq[EdmAgent] = {
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
      .map(nameOnlyAgent) // done but might need to add this ridiculous set of label={value} filters to the mapping
  }

  override def date(data: Document[NodeSeq]): Seq[EdmTimeSpan] = extractDate(data)

  override def description(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(data \ "freetext" \ "notes") // done

  override def extent(data: Document[NodeSeq]): ZeroToMany[String] =
    (data \ "freetext" \ "physicalDescription")
      .filter(node => filterAttribute(node, "label", "Dimensions"))
      .flatMap(extractStrings)

  override def format(data: Document[NodeSeq]): ZeroToMany[String] =
    (data \ "freetext" \ "physicalDescription")
      .filter(node => {
        filterAttribute(node, "label", "Physical description") || filterAttribute(node, "label", "Medium")
      }).flatMap(extractStrings)

  override def identifier(data: Document[NodeSeq]): ZeroToMany[String] =
    for {
      identifierProp <- data \ "freetext" \ "identifier"
      identifier <- extractStrings(identifierProp)
      attrValue <- identifierProp.attribute("label").flatMap(extractString(_))
      if attrValue.startsWith("Catalog") || attrValue.startsWith("Accession")
    } yield identifier // done

  override def language(data: Document[NodeSeq]): ZeroToMany[SkosConcept] =
    extractStrings(data \\ "indexedStructured" \ "language")
      .map(_.replaceAll(" language", "")) // remove ' language' from term
      .map(nameOnlyConcept) // done

  private def placeExtractor(node: NodeSeq, level: String, `type`: String): String = {
    getByAttribute(node \ level, "type", `type`)
      .flatMap(extractStrings(_))
      .mkString(", ")
  }

  override def place(data: Document[NodeSeq]): ZeroToMany[DplaPlace] = {
    // indexedStructured \ geoLocation > get dict values from this prop

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

    val geoLoc = (data \ "indexedStructured" \ "geoLocation").map(node => {
      val country = (getByAttribute(node \ "L2", "type", "Country") ++ getByAttribute(node \ "L2", "type", "Nation"))
        .flatMap(extractStrings(_))
        .mkString(", ")

      val state = (getByAttribute(node \ "L3", "type", "State") ++ getByAttribute(node \ "L3", "type", "Province"))
        .flatMap(extractStrings(_))
        .mkString(", ")

      val county = (getByAttribute(node \ "L4", "type", "County") ++ getByAttribute(node \ "L4", "type", "Island"))
        .flatMap(extractStrings(_))
        .mkString(", ")

      val city = (getByAttribute(node \ "L5", "type", "City") ++ getByAttribute(node \ "L5", "type", "Town"))
        .flatMap(extractStrings(_))
        .mkString(", ")

      val region = extractStrings(node \ "Other").mkString(", ")

      val lat = (node \ "points" \ "point" \ "latitude")
        .filter(node => filterAttribute(node, "type", "decimal") | filterAttribute(node, "type", "degrees"))
        .flatMap(extractStrings(_))
      val long = (node \ "points" \ "point" \ "longitude")
        .filter(node => filterAttribute(node, "type", "decimal") | filterAttribute(node, "type", "degrees"))
        .flatMap(extractStrings(_))
      val points = lat.zipAll(long, None, None).mkString(", ")

      DplaPlace(
        country = Some(country),
        state = Some(state),
        county = Some(county),
        region = Some(region),
        city = Some(city),
        coordinates = Some(points)
      )
    })

    // return
    if(geoLoc.isEmpty)
      (data \ "freetext" \ "place")
        .flatMap(extractStrings)
        .map(nameOnlyPlace)
    else
      geoLoc
  }

  override def publisher(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    (data \ "freetext" \ "publisher")
      .filter(node => filterAttribute(node, "label", "publisher"))
      .flatMap(extractStrings)
      .map(nameOnlyAgent) // done

  override def rights(data: Document[NodeSeq]): AtLeastOne[String] = {
    val mediaRights = (data \ "descriptiveNonRepeating" \ "online_media" \ "media")
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
  } // done

  override def subject(data: Document[NodeSeq]): ZeroToMany[SkosConcept] = {
    val subjectProps = Seq("topic", "name", "culture", "tax_kingdom", "tax_phylum", "tax_division", "tax_class",
      "tax_order", "tax_family", "tax_sub-family", "scientific_name", "common_name", "strat_group", "strat_formation",
      "strat_member")
    val topicAttrLabels = Seq("Topic", "subject", "event")

    (subjectProps.flatMap(subjectProp => extractStrings(data \ "indexedStructured" \ subjectProp)) ++
      topicAttrLabels.flatMap(topic => {
        (data \ "freetext" \ "topic")
          .flatMap(node => getByAttribute(node, "label", topic))
          .flatMap(extractStrings(_))
      })
    ).flatMap(_.splitAtDelimiter("\\\\"))
     .flatMap(_.splitAtDelimiter(":"))
     .map(nameOnlyConcept)
  } // done

  override def temporal(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] = extractDate(data)

  override def title(data: Document[NodeSeq]): AtLeastOne[String] =
    (data \ "descriptiveNonRepeating" \ "title")
      .filter(node =>
        filterAttribute(node, "label", "title") ||
        filterAttribute(node, "label", "object name") ||
        filterAttribute(node, "label", "title (spanish)"))
      .flatMap(node => extractStrings(node)) // done

  override def `type`(data: Document[NodeSeq]): ZeroToMany[String] =
    (data \ "freetext" \ "objectType")
      .flatMap(node => getByAttribute(node, "label", "Type"))
      .flatMap(extractStrings(_)) ++
      extractStrings(data \ "freetext" \ "physicalDescription") ++
      extractStrings(data \ "indexedStructure" \ "object_type") // done

  // Helper methods
  private def agent = EdmAgent(
    name = Some("Smithsonian Institution"),
    uri = Some(URI("http://dp.la/api/contributor/smithsonian"))
  ) // done

  private def extractDate(data: Document[NodeSeq]): Seq[EdmTimeSpan] =
    (data \ "freetext" \ "date")
      .filter(node => node.attributes.get("label").nonEmpty)
      .flatMap(extractStrings(_))
      .map(stringOnlyTimeSpan) // done

  private def extractPreview(data: Document[NodeSeq]): Seq[EdmWebResource] =
    (data \ "descriptiveNonRepeating" \ "online_media" \ "media")
      .flatMap(node => node.attribute("thumbnail"))
      .flatMap(node => extractStrings(node))
      .map(stringOnlyWebResource)
}


