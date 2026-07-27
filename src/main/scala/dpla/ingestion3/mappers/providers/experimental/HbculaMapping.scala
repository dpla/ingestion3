/**
 * TEST HUB — NOT APPROVED FOR PRODUCTION
 *
 * This mapper is under evaluation and has not been approved for inclusion
 * in the DPLA production index. Do not remove the `status = test` flag
 * from i3.conf until the hub has been formally approved.
 */
package dpla.ingestion3.mappers.providers.experimental

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.mappers.utils.{Document, XmlExtractor, XmlMapping}
import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.xml._

class HbculaMapping extends XmlMapping with XmlExtractor {

  // ID minting functions
  override def useProviderName: Boolean = true

  override def getProviderName: Option[String] = Some("hbcula")

  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] =
    extractString(data \ "header" \ "identifier")

  // SourceResource mapping

  override def collection(data: Document[NodeSeq]): ZeroToMany[DcmiTypeCollection] =
    extractStrings(data \ "metadata" \\ "isPartOf")
      .map(nameOnlyCollection)

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

  override def place(data: Document[NodeSeq]): ZeroToMany[DplaPlace] =
    extractStrings(data \ "metadata" \\ "spatial")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyPlace)

  override def rights(data: Document[NodeSeq]): AtLeastOne[String] =
    extractStrings(data \ "metadata" \\ "rights")

  override def subject(data: Document[NodeSeq]): ZeroToMany[SkosConcept] =
    extractStrings(data \ "metadata" \\ "subject")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyConcept)

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
    // dc:source holds the institution name for most sets. Some sets (e.g. rwwl)
    // omit it from the OAI export even though the CONTENTdm Repository field is
    // populated — this is a data issue to raise with HBCULA, not something the
    // mapper should paper over. Records without dc:source will fail the required
    // field check cleanly.
    extractStrings(data \ "metadata" \\ "source")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyAgent)
      .slice(0, 1)

  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    extractStrings(data \ "metadata" \\ "identifier")
      .filter(_.startsWith("http"))
      .map(stringOnlyWebResource)
      .slice(0, 1)

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] =
    Utils.formatXml(data)

  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] = {
    val url: Option[String] =
      extractStrings(data \ "metadata" \\ "identifier")
        .find(_.startsWith("http"))

    // CONTENTdm item URLs are structured as:
    //   http://hbcudigitallibrary.auctr.edu/cdm/ref/collection/{collection}/id/{id}
    // Thumbnail URLs are:
    //   http://hbcudigitallibrary.auctr.edu/utils/getthumbnail/collection/{collection}/id/{id}
    val parts: Seq[String] = url.getOrElse("").stripSuffix("/").split("/")
    val collection: Option[String] = parts.reverse.lift(2) // e.g. "ASUD"
    val item: Option[String] = parts.lastOption            // e.g. "0"

    if (collection.isDefined && item.isDefined) {
      val thumbUrl: String =
        "http://hbcudigitallibrary.auctr.edu/utils/getthumbnail/collection/" +
          collection.get + "/id/" + item.get
      Seq(stringOnlyWebResource(thumbUrl))
    } else Seq()
  }

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = agent

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(data))

  def agent: EdmAgent = EdmAgent(
    name = Some("HBCU Library Alliance"),
    uri = Some(URI("http://dp.la/api/contributor/hbcula"))
  )
}
