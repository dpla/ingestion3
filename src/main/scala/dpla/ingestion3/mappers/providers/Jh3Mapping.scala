package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.mappers.utils.{Document, MappingException, XmlExtractor, XmlMapping}
import dpla.ingestion3.messages.IngestMessageTemplates
import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.xml._

class Jh3Mapping
    extends XmlMapping
    with XmlExtractor
    with IngestMessageTemplates {

  override def useProviderName: Boolean = true

  override def getProviderName: Option[String] = Some("jh3")

  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] =
    extractString(data \ "header" \ "identifier")

  override def dplaUri(data: Document[NodeSeq]): ZeroToOne[URI] =
    mintDplaItemUri(data)

  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    extractStrings(metadataRoot(data) \ "Aggregation" \ "dataProvider")
      .map(nameOnlyAgent)

  override def edmRights(data: Document[NodeSeq]): ZeroToMany[URI] =
    (metadataRoot(data) \ "Aggregation" \ "rights")
      .flatMap(node => getAttributeValue(node, s"rdf:resource"))
      .map(URI)

  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] = {
    // Many CJH-member sets (LBI, AJHS_text, YIVO, etc.) omit edm:isShownAt but
    // provide edm:aggregatedCHO, which resolves to the public item page via the
    // CJH link resolver (http://digital.cjh.org/<id> → linkresolver.cjh.org).
    // Fall back to aggregatedCHO so these records are not lost.
    val aggregation = metadataRoot(data) \ "Aggregation"
    val explicit = (aggregation \ "isShownAt")
      .flatMap(node => getAttributeValue(node, "rdf:resource"))
    lazy val fallback = (aggregation \ "aggregatedCHO")
      .flatMap(node => getAttributeValue(node, "rdf:resource"))
    (if (explicit.nonEmpty) explicit else fallback).map(stringOnlyWebResource)
  }

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] =
    Utils.formatXml(data)

  // SourceResource
  override def collection(
      data: Document[NodeSeq]
  ): ZeroToMany[DcmiTypeCollection] =
    extractStrings(metadataRoot(data) \ "ProvidedCHO" \ "isPartOf")
      .map(nameOnlyCollection)

  override def creator(data: Document[NodeSeq]): ZeroToMany[EdmAgent] = {
    val agents = (metadataRoot(data) \ "Agent").map(agent => {
      // TODO rdaGr2:dateOfBirth and rdaGr2:dateOfDeath...?
      val name = extractString(agent \ "prefLabel")
      val note = extractString(agent \ "note")
      val uri = getAttributeValue(agent, "rdf:about").map(URI)
      val sameAs = (agent \ "sameAs")
        .flatMap(node => getAttributeValue(node, "rdf:resource"))
        .map(URI)
      EdmAgent(
        name = name,
        note = note,
        uri = uri,
        exactMatch = sameAs
      )
    })

    val creators =
      extractStrings(metadataRoot(data) \ "ProvidedCHO" \ "creator")
        .map(nameOnlyAgent)

    // Similar values exist in both dc:creators and edm:Agent, prefer to use edm:Agent
    if (agents.nonEmpty)
      agents
    else
      creators
  }

  override def contributor(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    extractStrings(metadataRoot(data) \ "ProvidedCHO" \ "contributor")
      .map(nameOnlyAgent)

  override def date(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
    extractStrings(metadataRoot(data) \ "ProvidedCHO" \ "date")
      .map(stringOnlyTimeSpan)

  // TODO filter by language...?
  override def description(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(metadataRoot(data) \ "ProvidedCHO" \ "description")

  override def format(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(metadataRoot(data) \ "ProvidedCHO" \ "format")

  override def identifier(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(metadataRoot(data) \ "ProvidedCHO" \ "identifier")

  override def language(data: Document[NodeSeq]): ZeroToMany[SkosConcept] =
    extractStrings(metadataRoot(data) \ "ProvidedCHO" \ "language")
      .map(nameOnlyConcept)

  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    isShownByUrls(data).map { url =>
      val sep = if (url.contains("?")) "&" else "?"
      stringOnlyWebResource(url + sep + "width=400")
    }

  override def mediaMaster(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    isShownByUrls(data).map(stringOnlyWebResource)

  override def publisher(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    extractStrings(metadataRoot(data) \ "ProvidedCHO" \ "publisher")
      .map(nameOnlyAgent)

  // Fail records that contain Mojibake (UTF-8 bytes misread as CP1252).
  // JH3 does not provide rights text — the throw path is the only effect
  // for affected records; clean records return an empty Seq as expected.
  override def rights(data: Document[NodeSeq]): ZeroToMany[String] = {
    if (Jh3Mapping.MojibakePattern.findFirstIn(data.get.text).isDefined)
      throw MappingException(
        "Mojibake encoding detected: UTF-8 bytes were misread as CP1252. " +
          "Please re-encode source records as UTF-8 and re-harvest."
      )
    Seq()
  }

  override def place(data: Document[NodeSeq]): ZeroToMany[DplaPlace] = {
    val placeConcepts = (metadataRoot(data) \ "Place").map(node => {
      val prefLabel = extractString(node \ "prefLabel")
      val coordinates =
        (extractString(node \ "lat"), extractString(node \ "long")) match {
          case (Some(lat: String), Some(long: String)) => Some(s"$lat,$long")
          case _                                       => None
        }
      DplaPlace(
        name = prefLabel,
        coordinates = coordinates
      )
    })

    val spatial = extractStrings(metadataRoot(data) \ "ProvidedCHO" \ "spatial")
      .map(nameOnlyPlace)

    // Similar values can exist in both edm:Place and dc:spatial, use edm:Place if defined otherwise dc:spatial
    if (placeConcepts.nonEmpty)
      placeConcepts
    else
      spatial
  }

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = agent

  // Concept
  override def subject(data: Document[NodeSeq]): ZeroToMany[SkosConcept] = {
    val concepts = (metadataRoot(data) \ "Concept").map(node => {
      val prefLabel = extractString(node \ "prefLabel")
      val note = extractString(node \ "note")
      val uri = getAttributeValue(node, "rdf:about").toSeq.map(URI)

      SkosConcept(
        providedLabel = prefLabel,
        note = note,
        exactMatch = uri
      )
    })

    val subjects = extractStrings(
      metadataRoot(data) \ "ProvidedCHO" \ "subject"
    ).map(nameOnlyConcept)
    // Similar values exist in skos:Concept and dc:subject, prefer skos:Concept over dc:subject
    if (concepts.nonEmpty)
      concepts
    else
      subjects
  }

  // TODO filter by language..?
  override def title(data: Document[NodeSeq]): AtLeastOne[String] =
    extractStrings(metadataRoot(data) \ "ProvidedCHO" \ "title")

  override def `type`(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(metadataRoot(data) \ "ProvidedCHO" \ "type")
      .flatMap(_.splitAtDelimiter(";"))

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(
      data
    ))

  private def agent = EdmAgent(
    name = Some("Jewish Heritage and History Hub"),
    uri = Some(URI("http://dp.la/api/contributor/jh3"))
  )

  private def isShownByUrls(data: Document[NodeSeq]): ZeroToMany[String] =
    (metadataRoot(data) \ "Aggregation" \ "isShownBy")
      .flatMap(node => getAttributeValue(node, "rdf:resource"))

  private def metadataRoot(data: Document[NodeSeq]): NodeSeq =
    data \ "metadata" \ "RDF"
}

object Jh3Mapping {
  // Compiled once at class load, not per record.
  // First class: CP1252 representations of UTF-8 first bytes for the scripts
  //   carried by JH3:
  //   - 0xC0–0xC3 (À–Ã): Latin extended (U+0000–U+00FF)
  //   - 0xD0–0xD3 (Ð–Ó): Cyrillic (U+0400–U+04FF)
  //   - 0xD7      (×):   Hebrew   (U+05D0–U+05FF)
  // Second class: CP1252 representations of UTF-8 continuation bytes 0x80–0xBF:
  //   - 0xA0–0xBF decode to U+00A0–U+00BF (identical to Latin-1)
  //   - 0x80–0x9F decode to scattered Unicode points (€, Ÿ, –, „, etc.),
  //     NOT to U+0080–U+009F, so those 27 code points are listed explicitly.
  val MojibakePattern =
    "[\\u00c0-\\u00c3\\u00d0-\\u00d3\\u00d7][\\u00a0-\\u00bf\\u0152\\u0153\\u0160\\u0161\\u0178\\u017d\\u017e\\u0192\\u02c6\\u02dc\\u2013\\u2014\\u2018\\u2019\\u201a\\u201c\\u201d\\u201e\\u2020\\u2021\\u2022\\u2026\\u2030\\u2039\\u203a\\u20ac\\u2122]".r
}
