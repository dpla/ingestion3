package dpla.eleanor.mappers

import dpla.ingestion3.mappers.utils.{Document, XmlExtractor, XmlMapping}
import dpla.ingestion3.messages.IngestMessageTemplates
import dpla.ingestion3.model.DplaMapData.{ExactlyOne, ZeroToMany, ZeroToOne}
import dpla.ingestion3.model.{EdmAgent, EdmWebResource, SkosConcept, URI, nameOnlyConcept, stringOnlyWebResource}
import dpla.ingestion3.utils.Utils
import org.json4s.JValue

import scala.xml.NodeSeq
import org.json4s.JsonDSL._

class StandardEbooksMapping extends XmlMapping with XmlExtractor
  with IngestMessageTemplates {

  override def useProviderName: Boolean = true

  override def getProviderName: Option[String] = Some("standard-ebooks")

  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] =
    extractString(data \ "id").map(_.trim)

  override def creator(xml: Document[NodeSeq]): Seq[EdmAgent] =
    (xml \ "author").map(author => {
      EdmAgent(
        name = extractString(author \ "name"),
        providedLabel = extractString(author \ "name"),
        exactMatch = (
          extractStrings(author \ "uri") ++
            extractStrings(author \ "sameAs")
          ).map(URI)
      )
    })

  override def subject(xml: Document[NodeSeq]): Seq[SkosConcept] =
    (xml \ "category").map(category => SkosConcept(
      concept = Option(category \@ "term"),
      providedLabel = Option(category \@ "term"),
      scheme = Option(category \@ "scheme").map(URI)
    ))

  override def dplaUri(data: Document[NodeSeq]): ZeroToOne[URI] =
    mintDplaItemUri(data)

  override def language(xml: Document[NodeSeq]): Seq[SkosConcept] =
    extractStrings(xml \ "language")
      .map(nameOnlyConcept)

  override def description(xml: Document[NodeSeq]): Seq[String] =
    extractStrings(xml \ "summary")

  override def title(xml: Document[NodeSeq]): Seq[String] =
    extractStrings(xml \ "title")

  override def edmRights(xml: Document[NodeSeq]): ZeroToMany[URI] =
    Seq(URI("http://rightsstatements.org/vocab/NoC-US/1.0/"))

  override def rights(xml: Document[NodeSeq]): Seq[String] =
    extractStrings(xml \ "rights")

  override def provider(xml: Document[NodeSeq]): ExactlyOne[EdmAgent] =
    EdmAgent(
      name = Some("Standard Ebooks"),
      uri = Some(URI("http://dp.la/api/contributor/standardebooks"))
    )

  override def publisher(xml: Document[NodeSeq]): Seq[EdmAgent] =
    Seq(provider(xml))

  override def `type`(data: Document[NodeSeq]): Seq[String] =
    Seq("text")

  override def format(data: Document[NodeSeq]): Seq[String] =
    Seq("Ebook")

  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    extractStrings(data \ "id").map(stringOnlyWebResource)

  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    (data \ "link")
      .filter(elem => (elem \@ "rel") == "http://opds-spec.org/image/thumbnail")
      .flatMap(elem => Option(elem \@ "href"))
      .map(href => "http://standardebooks.org" + href)
      .map(stringOnlyWebResource)


  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(data))

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] =
    Utils.formatXml(data)
}
