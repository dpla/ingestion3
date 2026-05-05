package dpla.ingestion3.mappers.providers

import com.typesafe.scalalogging.LazyLogging
import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.mappers.utils.{Document, XmlExtractor, XmlMapping}
import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.xml._

object MarylandMapping {
  // Matches a CC or RightsStatements URI in free text
  val rightsUriPattern =
    """https?://(?:creativecommons\.org/licenses/[^\s<>"]+|rightsstatements\.org/vocab/[^\s<>"]+)""".r

  // CC BY-ND 3.0 is described by name in Maryland accessRights text, not by URI
  val ccByNd30Patterns = Seq("attribution-noderivs 3.0", "attribution-noderivatives 3.0")

  val ccByNd30Uri = "https://creativecommons.org/licenses/by-nd/3.0/"
}

class MarylandMapping extends XmlMapping with XmlExtractor with LazyLogging {

  import MarylandMapping._

  // ID minting functions
  override def useProviderName: Boolean = true

  override def getProviderName: Option[String] = Some("maryland")

  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] =
    extractString(data \ "header" \ "identifier")

  // SourceResource mapping

  override def contributor(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    extractStrings(data \ "metadata" \\ "contributor")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyAgent)

  override def creator(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    extractStrings(data \ "metadata" \\ "creator")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyAgent)

  override def date(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
    extractStrings(data \ "metadata" \\ "date")
      .flatMap(_.splitAtDelimiter(";"))
      .map(stringOnlyTimeSpan)

  override def description(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(data \ "metadata" \\ "description")

  override def format(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(data \ "metadata" \\ "format")
      .map(_.stripSuffix(";"))

  override def language(data: Document[NodeSeq]): ZeroToMany[SkosConcept] =
    extractStrings(data \ "metadata" \\ "language")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyConcept)

  override def publisher(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    extractStrings(data \ "metadata" \\ "publisher")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyAgent)

  override def subject(data: Document[NodeSeq]): ZeroToMany[SkosConcept] =
    extractStrings(data \ "metadata" \\ "subject")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyConcept)

  override def temporal(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
    extractStrings(data \ "metadata" \\ "temporal")
      .flatMap(_.splitAtDelimiter(";"))
      .map(stringOnlyTimeSpan)

  override def title(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(data \ "metadata" \\ "title")
      .map(_.stripSuffix("."))

  override def `type`(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(data \ "metadata" \\ "type")
      .flatMap(_.splitAtDelimiter(";"))

  // OreAggregation
  override def dplaUri(data: Document[NodeSeq]): ZeroToOne[URI] =
    mintDplaItemUri(data)

  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    extractStrings(data \ "metadata" \\ "source")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyAgent)
      .slice(0, 1) // get first instance

  private def rightsUriFromText(text: String): Option[String] =
    rightsUriPattern.findFirstIn(text).orElse {
      val lower = text.toLowerCase
      if (ccByNd30Patterns.exists(lower.contains)) Some(ccByNd30Uri)
      else None
    }

  override def edmRights(data: Document[NodeSeq]): ZeroToMany[URI] = {
    val fromRights = extractStrings(data \ "metadata" \\ "rights").map(URI)
    if (fromRights.nonEmpty) fromRights
    else
      extractStrings(data \ "metadata" \\ "accessRights")
        .flatMap(rightsUriFromText)
        .map(URI)
        .slice(0, 1)
  }

  override def rights(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(data \ "metadata" \\ "accessRights")

  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    extractStrings(data \ "metadata" \\ "identifier")
      .filter(Utils.isUrl)
      .map(stringOnlyWebResource)
      .slice(0, 1)

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] =
    Utils.formatXml(data)

  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] = {
    val url: Option[String] = extractStrings(data \ "metadata" \\ "identifier")
      .filter(Utils.isUrl)
      .headOption

    url.flatMap { u =>
      val uri = new java.net.URI(u)
      val base = s"${uri.getScheme}://${uri.getHost}"
      val thumbnailPath = uri.getPath
        .replaceFirst("/cdm/ref/collection/", "/utils/getthumbnail/collection/")
      if (thumbnailPath.contains("getthumbnail"))
        Some(stringOnlyWebResource(base + thumbnailPath))
      else {
        logger.warn(s"MarylandMapping.preview: identifier URL did not match expected CONTENTdm path pattern, no thumbnail generated. URL: $u")
        None
      }
    }.toSeq
  }

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = agent

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(
      data
    ))

  // Helper method
  def agent: EdmAgent = EdmAgent(
    name = Some("Digital Maryland"),
    uri = Some(URI("http://dp.la/api/contributor/maryland"))
  )
}
