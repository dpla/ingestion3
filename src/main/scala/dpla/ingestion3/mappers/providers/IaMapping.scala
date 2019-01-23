package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils._
import dpla.ingestion3.messages.IngestMessageTemplates
import dpla.ingestion3.model.DplaMapData.{AtLeastOne, ExactlyOne, ZeroToMany, ZeroToOne}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.json4s
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._

class IaMapping extends JsonMapping with JsonExtractor with IngestMessageTemplates {

  // ID minting functions
  override def useProviderName: Boolean = true

  override def getProviderName: String = "ia"

  override def originalId(implicit data: Document[JValue]): ExactlyOne[String] =
    extractString(unwrap(data) \ "identifier")
       .getOrElse(throw new RuntimeException(s"No ID for record: ${compact(data)}"))

  // OreAggregration
  override def dataProvider(data: Document[JValue]): ZeroToMany[EdmAgent] = {
    val candidateDataProviders =
      extractStrings(unwrap(data) \\ "contributor")
        .map(nameOnlyAgent)
    if (candidateDataProviders isEmpty)
      Seq(nameOnlyAgent("Internet Archive"))
    else
      candidateDataProviders
  }


  override def dplaUri(data: Document[JValue]): ExactlyOne[URI] = URI(mintDplaId(data))

  override def edmRights(data: Document[json4s.JValue]): ZeroToMany[URI] =
    extractStrings(unwrap(data) \\ "licenseurl").map(URI)

  override def intermediateProvider(data: Document[JValue]): ZeroToOne[EdmAgent] =
    extractStrings(unwrap(data) \\ "collection").flatMap {
      // transforms institution shortnames into properly formatted names
      case "medicalheritagelibrary" => Some("Medical Heritage Library")
      case "blc" => Some("Boston Library Consortium")
      case _ => None
    }
      .map(nameOnlyAgent)
      .headOption

  override def isShownAt(data: Document[JValue]): ZeroToMany[EdmWebResource] =
    extractStrings(unwrap(data) \\ "identifier")
      .map(identifier => stringOnlyWebResource("http://www.archive.org/details/" + identifier))

  override def originalRecord(data: Document[JValue]): ExactlyOne[String] = Utils.formatJson(data)

  override def preview(data: Document[JValue]): ZeroToMany[EdmWebResource] =
    extractStrings(unwrap(data) \\ "identifier")
      .map(identifier => stringOnlyWebResource("https://archive.org/services/img/" + identifier))

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

  override def rights(data: Document[JValue]): AtLeastOne[String] = {
    val defaultRights = Seq("Access to the Internet Archive’s Collections is granted for scholarship " +
      "and research purposes only. Some of the content available through the Archive may be governed " +
      "by local, national, and/or international laws and regulations, and your use of such content " +
      "is solely at your own risk")
    val rights = extractStrings(unwrap(data) \\ "rights") ++
      extractStrings(unwrap(data) \\ "possible-copyright-status")

    if(rights.nonEmpty) rights else defaultRights
  }

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
    uri = Some(URI("http://dp.la/api/contributor/ia"))
  )
}
