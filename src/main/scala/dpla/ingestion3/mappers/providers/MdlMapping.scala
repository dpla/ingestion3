package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.enrichments.normalizations.filters.ExtentIdentificationList
import dpla.ingestion3.mappers.utils._
import dpla.ingestion3.messages.IngestMessageTemplates
import dpla.ingestion3.model.DplaMapData.{AtLeastOne, ExactlyOne, ZeroToMany, ZeroToOne}
import dpla.ingestion3.model._
import org.json4s.JValue
import org.json4s.JsonDSL._
import dpla.ingestion3.utils.Utils
import org.json4s.jackson.JsonMethods

import scala.util.{Success, Try}

class MdlMapping
    extends JsonMapping
    with JsonExtractor
    with IngestMessageTemplates {
  protected lazy val formatBlockList: Set[String] =
    ExtentIdentificationList.termList
  override val enforceDuplicateIds: Boolean = false

  // ID minting functions
  override def useProviderName: Boolean = true

  override def getProviderName: Option[String] = Some("minnesota")

  override def originalId(implicit data: Document[JValue]): ZeroToOne[String] =
    extractString(unwrap(data) \ "attributes" \ "metadata" \ "isShownAt")

  // OreAggregation
  override def dataProvider(data: Document[JValue]): ZeroToMany[EdmAgent] =
    extractStrings(unwrap(data) \ "attributes" \ "metadata" \ "dataProvider")
      .map(nameOnlyAgent)

  override def dplaUri(data: Document[JValue]): ZeroToOne[URI] =
    mintDplaItemUri(data)

  override def edmRights(data: Document[JValue]): ZeroToMany[URI] =
    extractStrings(unwrap(data) \ "attributes" \ "metadata" \ "rights")
      .filter(r =>
        Try {
          new java.net.URI(r).getHost
        } match {
          case Success(host: String) =>
            host.equalsIgnoreCase("rightsstatements.org") || host
              .equalsIgnoreCase("creativecommons.org")
          case _ => false
        }
      )
      .map(URI)

  override def iiifManifest(data: Document[JValue]): ZeroToMany[URI] = {
    extractStrings(
      unwrap(
        data
      ) \ "attributes" \ "metadata" \ "jsonData" \ "response" \ "document" \ "iiif_manifest_ss"
    )
      .map(JsonMethods.parse(_))
      .flatMap(json => extractString(json \ "@id"))
      .map(URI)
  }

  override def intermediateProvider(
      data: Document[JValue]
  ): ZeroToOne[EdmAgent] =
    extractString(
      unwrap(data) \ "attributes" \ "metadata" \ "intermediateProvider"
    ).map(nameOnlyAgent)

  override def isShownAt(data: Document[JValue]): ZeroToMany[EdmWebResource] =
    extractStrings(unwrap(data) \ "attributes" \ "metadata" \ "isShownAt")
      .map(stringOnlyWebResource)

  override def originalRecord(data: Document[JValue]): ExactlyOne[String] =
    Utils.formatJson(data)

  override def preview(data: Document[JValue]): ZeroToMany[EdmWebResource] =
    extractStrings(unwrap(data) \ "attributes" \ "metadata" \ "object")
      .map(stringOnlyWebResource)

  override def provider(data: Document[JValue]): ExactlyOne[EdmAgent] = agent

  override def sidecar(data: Document[JValue]): JValue =
    ("prehashId", buildProviderBaseId()(data)) ~ ("dplaId", mintDplaId(data))

  // SourceResource
  override def collection(
      data: Document[JValue]
  ): ZeroToMany[DcmiTypeCollection] =
    extractCollection(
      unwrap(data) \ "attributes" \ "metadata" \ "sourceResource" \ "collection"
    )

  override def contributor(data: Document[JValue]): ZeroToMany[EdmAgent] =
    extractStrings(
      unwrap(
        data
      ) \ "attributes" \ "metadata" \ "sourceResource" \ "contributor"
    ).map(nameOnlyAgent)

  override def creator(data: Document[JValue]): ZeroToMany[EdmAgent] =
    extractStrings(
      unwrap(data) \ "attributes" \ "metadata" \ "sourceResource" \ "creator"
    ).map(nameOnlyAgent)

  override def date(data: Document[JValue]): ZeroToMany[EdmTimeSpan] =
    extractDate(
      unwrap(data) \ "attributes" \ "metadata" \ "sourceResource" \ "date"
    )

  override def description(data: Document[JValue]): ZeroToMany[String] =
    extractStrings(
      unwrap(
        data
      ) \ "attributes" \ "metadata" \ "sourceResource" \ "description"
    )

  override def extent(data: Document[JValue]): ZeroToMany[String] =
    extractStrings(
      unwrap(data) \ "attributes" \ "metadata" \ "sourceResource" \ "extent"
    )

  override def format(data: Document[JValue]): ZeroToMany[String] =
    extractStrings(
      unwrap(data) \ "attributes" \ "metadata" \ "sourceResource" \ "format"
    )
      .map(_.applyBlockFilter(formatBlockList))
      .filter(_.nonEmpty)

  override def genre(data: Document[JValue]): ZeroToMany[SkosConcept] =
    extractStrings(
      unwrap(data) \ "attributes" \ "metadata" \ "sourceResource" \ "type"
    ).map(nameOnlyConcept)

  override def language(data: Document[JValue]): ZeroToMany[SkosConcept] = {
    val codes = extractStrings(
      unwrap(
        data
      ) \ "attributes" \ "metadata" \ "sourceResource" \ "language" \ "iso639_3"
    )
    val names = extractStrings(
      unwrap(
        data
      ) \ "attributes" \ "metadata" \ "sourceResource" \ "language" \ "name"
    )

    (codes.nonEmpty, names.nonEmpty) match {
      case (true, _) =>
        codes.map(nameOnlyConcept) // if iso639_3 is given use that value
      case (false, true) =>
        names.map(
          nameOnlyConcept
        ) // if iso639_3 is not given and name is, use name
      case (_, _) => List() // empty list
    }
  }

  override def place(data: Document[JValue]): ZeroToMany[DplaPlace] =
    place(
      unwrap(data) \ "attributes" \ "metadata" \ "sourceResource" \ "spatial"
    )

  override def publisher(data: Document[JValue]): ZeroToMany[EdmAgent] =
    extractStrings(
      unwrap(data) \ "attributes" \ "metadata" \ "sourceResource" \ "publisher"
    ).map(nameOnlyAgent)

  override def rights(data: Document[JValue]): AtLeastOne[String] = {
    extractStrings(unwrap(data) \ "attributes" \ "metadata" \ "rights" ++ unwrap(data) \ "attributes" \ "metadata" \ "sourceResource" \ "rights")
      .filterNot(r =>
        Try {
          new java.net.URI(r).getHost
        } match {
          case Success(host: String) =>
            host.equalsIgnoreCase("rightsstatements.org") || host
              .equalsIgnoreCase("creativecommons.org")
          case _ => false
        }
      )
  }

  override def subject(data: Document[JValue]): ZeroToMany[SkosConcept] =
    extractStrings(
      unwrap(
        data
      ) \ "attributes" \ "metadata" \ "sourceResource" \ "subject" \ "name"
    ).map(nameOnlyConcept)

  override def title(data: Document[JValue]): AtLeastOne[String] =
    extractStrings(
      unwrap(data) \ "attributes" \ "metadata" \ "sourceResource" \ "title"
    )

  override def `type`(data: Document[JValue]): ZeroToMany[String] =
    extractStrings(
      unwrap(data) \ "attributes" \ "metadata" \ "sourceResource" \ "type"
    )

  // Helper functions
  def extractCollection(collection: JValue): ZeroToMany[DcmiTypeCollection] = {
    iterify(collection).children.map(c => {
      DcmiTypeCollection(
        title = extractString(c \\ "title"),
        description = extractString(c \\ "description"),
        isShownAt = None
      )
    })
  }

  def extractDate(date: JValue): ZeroToMany[EdmTimeSpan] = {
    iterify(date).children.map(d =>
      EdmTimeSpan(
        begin = extractString(d \ "begin"),
        end = extractString(d \ "end"),
        originalSourceDate = extractString(d \ "displayDate")
      )
    )
  }

  def place(place: JValue): ZeroToMany[DplaPlace] = {
    iterify(place).children.map(p =>
      DplaPlace(
        // use headOption because 'name' and 'coordinate' values can be either Strings or Lists in original data
        name = extractStrings(p \\ "name").headOption,
        coordinates = extractStrings(p \\ "coordinates").headOption
      )
    )
  }

  def agent: EdmAgent = EdmAgent(
    name = Some("Minnesota Digital Library"),
    uri = Some(URI("http://dp.la/api/contributor/mdl"))
  )
}
