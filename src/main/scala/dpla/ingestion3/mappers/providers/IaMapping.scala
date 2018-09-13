package dpla.ingestion3.mappers.providers

import java.net.URI

import dpla.ingestion3.mappers.utils._
import dpla.ingestion3.messages.{IngestMessage, IngestMessageTemplates, MessageCollector}
import dpla.ingestion3.model.DplaMapData.{AtLeastOne, ExactlyOne, ZeroToMany, ZeroToOne}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.json4s
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.util.{Failure, Success, Try}

// FIXME Why is the implicit conversion not working for JValue when it is for NodeSeq?
class IaMapping extends JsonMapping with JsonExtractor with IdMinter[JValue] with IngestMessageTemplates {

  // ID minting functions
  override def useProviderName: Boolean = true

  override def getProviderName: String = "ia"

  override def getProviderId(implicit data: Document[JValue]): String =
    extractString(unwrap(data) \ "identifier")
       .getOrElse(throw new RuntimeException(s"No ID for record: ${compact(data)}"))

  // OreAggregration
  override def dataProvider(data: Document[JValue])
                           (implicit msgCollector: MessageCollector[IngestMessage]): ExactlyOne[EdmAgent] =
    extractString(unwrap(data) \\ "contributor").map(nameOnlyAgent) match {
      case Some(dp) => dp
      case None => msgCollector.add(missingRequiredError(getProviderId(data), "dataProvider"))
        nameOnlyAgent("") // FIXME this shouldn't have to return an empty value.
    }

  override def dplaUri(data: Document[JValue]): ExactlyOne[URI] = new URI(mintDplaId(data))

  override def edmRights(data: Document[json4s.JValue]): ZeroToOne[URI] =
    extractString(unwrap(data) \\ "licenseurl").map(new URI(_))

  override def isShownAt(data: Document[JValue])
                        (implicit msgCollector: MessageCollector[IngestMessage]): EdmWebResource =
    extractStrings(unwrap(data) \\ "identifier").flatMap(identifier => {
      val urlStr = "http://www.archive.org/details/" + identifier
      Try { new URI(urlStr)} match {
        case Success(uri) => Option(uriOnlyWebResource(uri))
        case Failure(f) =>
          msgCollector.add(
            mintUriError(id = getProviderId(data), field = "isShownAt", value = urlStr))
          None
      }
    }).headOption match {
      case None =>
        msgCollector.add(missingRequiredError(id = getProviderId(data), field = "isShownAt")) // record error message
        uriOnlyWebResource(new URI("")) // TODO Fix this -- it requires an Exception thrown or empty EdmWebResource
      case Some(s) => s
    }

  override def originalRecord(data: Document[JValue]): ExactlyOne[String] = Utils.formatJson(data)

  override def provider(data: Document[JValue]): ExactlyOne[EdmAgent] = agent

  override def sidecar(data: Document[JValue]): JValue =
    ("prehashId", buildProviderBaseId()(data)) ~ ("dplaId", mintDplaId(data))

  // SourceResource
  override def creator(data: Document[JValue]): ZeroToMany[EdmAgent] =
    extractStrings(unwrap(data)  \\ "creator").map(nameOnlyAgent)

  override def date(data: Document[JValue]): ZeroToMany[EdmTimeSpan] =
    extractStrings(unwrap(data)  \\ "date").map(stringOnlyTimeSpan)

  override def description(data: Document[JValue]): ZeroToMany[String] =
    extractStrings(unwrap(data) \\ "description")

  override def language(data: Document[JValue]): ZeroToMany[SkosConcept] =
    extractStrings(unwrap(data) \\ "language" ).map(nameOnlyConcept)

  override def publisher(data: Document[JValue]): ZeroToMany[EdmAgent] =
    extractStrings(unwrap(data)  \\ "publisher").map(nameOnlyAgent)

  override def rights(data: Document[_root_.org.json4s.JsonAST.JValue]): AtLeastOne[String] =
    extractStrings(unwrap(data) \\ "rights") ++
      extractStrings(unwrap(data) \\ "possible-copyright-status")

  override def subject(data: Document[JValue]): ZeroToMany[SkosConcept] =
    extractStrings(unwrap(data)  \\ "subject").map(nameOnlyConcept)

  override def title(data: Document[JValue]): AtLeastOne[String] = {
    val titles = extractStrings(unwrap(data)  \\ "title")
    val vols = extractStrings(unwrap(data)  \\ "volume")
    val issues = extractStrings(unwrap(data)  \\ "issue")

    val max = List(titles.size, vols.size, issues.size).max

    val t_max = titles.padTo(max, "")
    val v_max = vols.padTo(max, "")
    val i_max = issues.padTo(max, "")

    // Merges t_max, v_max and i_max together separated by comma.
    ((t_max zip v_max).map(a => (a._1.isEmpty, a._2.isEmpty) match {
      case (false, false) => s"${a._1}, ${a._2}"
      case (true, true) => ""
      case (true, false) => s"${a._2}"
      case (false, true) => s"${a._1}"
    }) zip i_max).map(a => (a._1.isEmpty, a._2.isEmpty) match {
      case (false, false) => s"${a._1}, ${a._2}"
      case (true, true) => ""
      case (true, false) => s"${a._2}"
      case (false, true) => s"${a._1}"
    })
  }

  override def `type`(data: Document[JValue]): ZeroToMany[String] =
    extractStrings(unwrap(data)  \\ "mediatype")

  def agent = EdmAgent(
    name = Some("Internet Archive"),
    uri = Some(new URI("http://dp.la/api/contributor/ia"))
  )
}
