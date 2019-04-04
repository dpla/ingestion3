package dpla.ingestion3.mappers.providers

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
  override def collection(data: Document[NodeSeq]): Seq[DcmiTypeCollection] = ???

  override def contributor(data: Document[NodeSeq]): Seq[EdmAgent] = ???

  override def creator(data: Document[NodeSeq]): Seq[EdmAgent] = ???

  override def date(data: Document[NodeSeq]): Seq[EdmTimeSpan] = ???

  override def description(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(data \ "freetext" \ "notes") // done

  override def extent(data: Document[NodeSeq]): ZeroToMany[String] = ???

  override def format(data: Document[NodeSeq]): ZeroToMany[String] = ???

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

  override def place(data: Document[NodeSeq]): ZeroToMany[DplaPlace] = ???

  override def publisher(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    (data \ "freetext" \ "publisher")
      .filter(node => filterAttribute(node, "label", "publisher"))
      .flatMap(extractStrings)
      .map(nameOnlyAgent) // done

  override def relation(data: Document[NodeSeq]): ZeroToMany[LiteralOrUri] = ???

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
    ).map(nameOnlyConcept)
  } // done

  override def temporal(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
    (data \ "freetext" \ "date")
      .filter(node => node.attributes.get("label").nonEmpty)
      .flatMap(extractStrings(_))
      .map(stringOnlyTimeSpan) // done

  override def title(data: Document[NodeSeq]): AtLeastOne[String] =
    (data \ "descriptiveNonRepeating" \ "title")
      .filter(node =>
        filterAttribute(node, "label", "title") ||
        filterAttribute(node, "label", "object name") ||
        filterAttribute(node, "label", "title (spanish)"))
      .flatMap(node => extractStrings(node)) // done

  override def `type`(data: Document[NodeSeq]): ZeroToMany[String] =
  // freetext \ objectType where @label='Type'
  // freetext \ physicalDescription
  // indexedStructure \ object_type
  {
    (data \ "freetext" \ "objectType")
      .flatMap(node => getByAttribute(node, "label", "Type"))
      .flatMap(extractStrings(_)) ++
    extractStrings(data \ "freetext" \ "physicalDescription") ++
    extractStrings(data \ "indexedStructure" \ "object_type")
  }


  // Helper methods
  private def agent = EdmAgent(
    name = Some("Smithsonian Institution"),
    uri = Some(URI("http://dp.la/api/contributor/smithsonian"))
  ) // done

  private def extractPreview(data: Document[NodeSeq]): Seq[EdmWebResource] =
    (data \ "descriptiveNonRepeating" \ "online_media" \ "media")
      .flatMap(node => node.attribute("thumbnail"))
      .flatMap(node => extractStrings(node))
      .map(stringOnlyWebResource)
}


