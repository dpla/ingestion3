package dpla.ingestion3.mappers

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.mappers.utils._
import dpla.ingestion3.messages.{
  IngestMessage,
  IngestMessageTemplates,
  MessageCollector
}
import dpla.ingestion3.model.DplaMapData.{ExactlyOne, ZeroToMany, ZeroToOne}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils._
import org.json4s.DefaultFormats
import org.json4s.JsonAST._

import scala.util.{Failure, Success, Try}
import scala.xml.NodeSeq

trait Mapper[T, +E] extends IngestMessageTemplates {
  implicit val formats: DefaultFormats.type = DefaultFormats

  def map(document: Document[T], mapping: Mapping[T]): OreAggregation

  /** Normalizes edmRights URIs and logs specific messages for each
    * transformation (logged as warnings)
    *   1. HTTPS > HTTP 2. Drop `www` 3. Change `/page/` to `/vocab/` 4. Drop
    *      query parameters (ex. ?lang=en) 5. Add missing `/` to end of URI 6.
    *      Remove trailing punctuation (ex.
    *      http://rightsstaments.org/vocab/Inc/;) 7. Leading and trailing
    *      whitespace also remove but no messages are logged for this
    *      transformation
    *
    * If original value is not a valid java.net.URI the original value is
    * returned
    *
    * @param values
    *   Seq[URI] edmRights URIs to be normalized
    * @param providerId
    *   String Provider supplied record identifier
    * @param collector
    *   MessageCollector Ingest message collector
    * @return
    *   Seq[URI] URIs
    */
  def normalizeEdmRights(values: ZeroToMany[URI], providerId: String)(implicit
      collector: MessageCollector[IngestMessage]
  ): ZeroToMany[URI] = {
    values.map(value => {
      val normalized = Try { new java.net.URI(value.toString.trim) } match {
        case Success(uri) =>
          Try {

            // does scheme (http/https) require normalization
            if (uri.toString.startsWith("https")) {
              collector.add(
                normalizedEdmRightsHttpsMsg(
                  providerId,
                  "edmRights",
                  value.toString,
                  enforce = false
                )
              )
            }
            // `www` to be removed?
            if (uri.toString.contains("www")) {
              collector.add(
                normalizedEdmRightsWWWMsg(
                  providerId,
                  "edmRights",
                  value.toString,
                  enforce = false
                )
              )
            }
            // change /page/ to /vocab/
            // remove /rdf
            val path =
              if (
                uri.getPath.contains("/page/") | uri.getPath.contains("/rdf")
              ) {
                collector.add(
                  normalizedEdmRightsRsPageMsg(
                    providerId,
                    "edmRights",
                    value.toString,
                    enforce = false
                  )
                )
                uri.getPath
                  .replaceFirst("/page/", "/vocab/")
                  .replace("/rdf", "")
              } else {
                uri.getPath
              }

            // Strip `?` and all following
            if (uri.getQuery != null) {
              collector.add(
                normalizedEdmRightsRsPageMsg(
                  providerId,
                  "edmRights",
                  value.toString,
                  enforce = false
                )
              )
            }
            // trailing `/` on path
            if (!uri.getPath.endsWith("/")) {
              collector.add(
                normalizedEdmRightsTrailingSlashMsg(
                  providerId,
                  "edmRights",
                  value.toString,
                  enforce = false
                )
              )
            }
            // trailing punctuation
            if (
              !uri.getPath.endsWith("/") &&
              !uri.getPath
                .equalsIgnoreCase(uri.getPath.cleanupEndingPunctuation)
            ) {
              collector.add(
                normalizedEdmRightsTrailingPunctuationMsg(
                  providerId,
                  "edmRights",
                  value.toString,
                  enforce = false
                )
              )
            }
            // force http, drop parameters and trailing punctuation
            val uriString =
              s"http://${uri.getHost}${path.cleanupEndingPunctuation}/"

            // normalize() drops duplicates //
            new java.net.URI(uriString).normalize.toString

          } match {
            case Success(normalizedUri) => normalizedUri
            case Failure(f)             =>
              // log warning message about failed normalization
              collector.add(
                invalidEdmRightsMsg(
                  providerId,
                  "edmRights",
                  s"${value.toString}: $f",
                  enforce = false
                )
              )
              // return original value
              value.toString
          }
        case Failure(_) => value.toString
      }

      URI(normalized)
    })
  }

  /** Performs validation checks against dataProvider If more than one value was
    * provided, log messages and return only the first value If no value was
    * provided, log a message and return an empty EdmAgent
    *
    * @param values
    *   Seq[EdmAgent] Values generated by provider mapping
    * @param providerId
    *   String Local provider identifier
    * @param enforce
    *   Boolean True Enforce this validation and fails records that do not pass
    *   with an error message False Enforces validation but logs warnings which
    *   will not fail a record.
    * @param collector
    *   MessageCollector Ingest message collector
    * @return
    *   EdmAgent Returns first EdmAgent or an empty EdmAgent if none mapped
    */
  def validateDataProvider(
      values: ZeroToMany[EdmAgent],
      providerId: String,
      enforce: Boolean
  )(implicit collector: MessageCollector[IngestMessage]): EdmAgent = {
    // Check for more than one dataProvider mapped
    // Do not fail record if multiple dataProviders mapped, only warning
    if (values.size > 1) {
      collector.add(
        moreThanOneValueMsg(
          providerId,
          "dataProvider",
          values.map(_.print).mkString(" | "),
          enforce = false
        )
      )
    }

    // Check for at least one value in required field
    values.headOption match {
      case Some(dataProvider) => dataProvider
      case None =>
        collector.add(
          missingRequiredFieldMsg(providerId, "dataProvider", enforce)
        )
        emptyEdmAgent
    }
  }

  /** Performs validation checks against edmRights values. Log a message if more
    * than one value was mapped. Checks that all values are valid URIs and have
    * domains of creativecommons.org or rightsstatements.org
    *
    * @param values
    *   Seq[URI] Values generated by edmRights mapping
    * @param providerId
    *   String Local provider identifier
    * @param enforce
    *   Boolean True Enforce this validation and fails records that do not pass
    *   with an error message False Enforces validation but logs warnings which
    *   will not fail a record.
    * @param collector
    *   MessageCollector Ingest message collector
    * @return
    *   Option[URI] Returns the first validated URIedmRights URI
    */
  def validateEdmRights(
      values: ZeroToMany[URI],
      providerId: String,
      enforce: Boolean
  )(implicit collector: MessageCollector[IngestMessage]): ZeroToOne[URI] = {
    // Do not fail record if more than one value mapped
    if (values.size > 1) {
      collector.add(
        moreThanOneValueMsg(
          providerId,
          "edmRights",
          values.mkString(" | "),
          enforce
        )
      )
    }

    values.foreach(value => {
      if (!value.isValidEdmRightsUri)
        collector.add(
          invalidEdmRightsMsg(
            providerId,
            "edmRights",
            value.toString,
            enforce
          )
        )
    })

    val validEdmRights = values.filter(_.isValidEdmRightsUri)

    if (validEdmRights.size > 1) {
      // multiple valid edmRights URIs provided in a single record, this should be a mapping failure
      collector.add(
        multipleEdmRightsMsg(
          providerId,
          "edmRights",
          validEdmRights.map(_.toString).mkString(" | "),
          enforce = true
        )
      )
    }

    values.find(_.isValidEdmRightsUri)
  }

  /** Performs validation checks against isShownAt Log a message if more than
    * one value was mapped from original record but do not fail the record Log a
    * message if no value was mapped from original record
    *
    * @param values
    *   Seq[EdmWebResource] Values generated by isShownAt mapping
    * @param providerId
    *   String Local provider identifier
    * @param enforce
    *   Boolean True Enforce this validation and fails records that do not pass
    *   with an error message False Enforces validation but logs warnings which
    *   will not fail a record.
    * @param collector
    *   MessageCollector Ingest message collector
    * @return
    *   ExactlyOne[EdmWebResource] First mapped EdmWebResource or empty
    *   EdmWebResource
    */
  def validateIsShownAt(
      values: ZeroToMany[EdmWebResource],
      providerId: String,
      enforce: Boolean
  )(implicit
      collector: MessageCollector[IngestMessage]
  ): ExactlyOne[EdmWebResource] = {
    // Do not fail record if more than one value mapped
    if (values.size > 1) {
      collector.add(
        moreThanOneValueMsg(
          providerId,
          "isShownAt",
          values.mkString(" | "),
          enforce = false
        )
      )
    }

    values.headOption match {
      case Some(url) => url
      case None =>
        collector.add(missingRequiredFieldMsg(providerId, "isShownAt", enforce))
        emptyEdmWebResource
    }
  }

  /** Performs validation checks against object values Logs a message if more
    * than one value provided and returns the head value
    *
    * TODO: Perform validation of EdmWebResource.uri value
    *
    * @param values
    *   Seq[EdmWebResource] Values generated by object mapping
    * @param providerId
    *   String Local provider identifier
    * @param enforce
    *   Boolean True Enforce this validation and fails records that do not pass
    *   with an error message False Enforces validation but logs warnings which
    *   will not fail a record.
    * @param collector
    *   MessageCollector Ingest message collector
    * @return
    *   Option[EdmWebResource] object EdmWebResource
    */
  def validateObject(
      values: ZeroToMany[EdmWebResource],
      providerId: String,
      enforce: Boolean
  )(implicit
      collector: MessageCollector[IngestMessage]
  ): ZeroToOne[EdmWebResource] = {
    if (values.size > 1) {
      collector.add(
        moreThanOneValueMsg(
          providerId,
          "object",
          values.toString(),
          enforce
        )
      )
    }
    values.headOption
  }

  /** Performs validation checks against preview values, expects only one value,
    * but if more than one value is provided then return the head value
    *
    * TODO: Perform validation of EdmWebResource.uri value
    *
    * @param values
    *   Seq[EdmWebResource] Values generated by preview mapping
    * @param providerId
    *   String Local provider identifier
    * @param enforce
    *   Boolean True Enforce this validation and fails records that do not pass
    *   with an error message False Enforces validation but logs warnings which
    *   will not fail a record.
    * @param collector
    *   MessageCollector Ingest message collector
    * @return
    *   Option[EdmWebResource] preview EdmWebResource
    */
  def validatePreview(
      values: ZeroToMany[EdmWebResource],
      providerId: String,
      enforce: Boolean
  )(implicit
      collector: MessageCollector[IngestMessage]
  ): ZeroToOne[EdmWebResource] = {
    if (values.size > 1) {
      collector.add(
        moreThanOneValueMsg(
          providerId,
          "preview",
          values.mkString(" | "),
          enforce
        )
      )
    }
    values.headOption
  }

  /** Checks for the presence of at least one value in a given field which is
    * optional according to DPLA MAP specification, but strongly recommended
    *
    * @param values
    *   Seq[T] Values
    * @param field
    *   String Name of field being validated
    * @param enforce
    *   Boolean True Enforce this validation and logs a warning for records
    *   missing this field False Does not perform any kind of checking for this
    *   field
    * @param providerId
    *   String Local provider identifier
    * @param collector
    *   MessageCollector Ingest message collector
    */
  def validateRecommendedProperty[U](
      values: ZeroToMany[U],
      field: String,
      providerId: String,
      enforce: Boolean
  )(implicit collector: MessageCollector[IngestMessage]): ZeroToMany[U] = {
    if (values.isEmpty && enforce) {
      collector.add(missingRecommendedFieldMsg(providerId, field))
    }
    values
  }

  /** Compares rights and edmRights and logs a message if neither is set. Also
    * checks if both fields contain a value and logs a message indicating
    * duplication of rights information
    *
    * @param rights
    *   Seq[String] Values returned from dc:rights mapping
    * @param edmRights
    *   Option[URI] Value returned from validateEdmRights
    * @param providerId
    *   String The provider's local identifier
    * @param enforce
    *   Boolean True Enforce this validation and fails records that do not pass
    *   with an error message False Enforces validation but logs warnings which
    *   will not fail a record.
    * @param collector
    *   Message collector Ingest message collector
    */
  def validateRights(
      rights: ZeroToMany[String],
      edmRights: ZeroToOne[URI],
      providerId: String,
      enforce: Boolean
  )(implicit collector: MessageCollector[IngestMessage]): Unit = {
    (rights.isEmpty, edmRights.isEmpty) match {
      // If both dc:rights and edmRights are not provided in the original record and the validation should be enforced
      // then this will log an error message, otherwise it will be logged as a warning
      case (true, true)   => collector.add(missingRights(providerId, enforce))
      case (false, false) => collector.add(duplicateRights(providerId))
      case (_, _)         => // do nothing
    }
  }

  /** Validates the presence of at least one title (required property per DPLA
    * MAP) and logs a missing required property message if the Seq is empty
    *
    * @param titles
    *   Seq[String] Values returned from rights mapping
    * @param providerId
    *   String The provider's local identifier
    * @param enforce
    *   Boolean True Enforce this validation and fails records that do not pass
    *   with an error message False Enforces validation but logs warnings which
    *   will not fail a record.
    * @param collector
    *   MessageCollector Ingest message collector
    */
  def validateTitle(
      titles: ZeroToMany[String],
      providerId: String,
      enforce: Boolean
  )(implicit collector: MessageCollector[IngestMessage]): ZeroToMany[String] = {
    if (titles.isEmpty) {
      collector.add(missingRequiredFieldMsg(providerId, "title", enforce))
    }
    titles
  }

  /** Performs validation checks against originalId values If no value was
    * provided, an empty String is return and an error message is logged
    *
    * @param values
    *   Seq[String] Values generated by provider mapping
    * @param providerId
    *   String Local provider identifier
    * @param enforce
    *   Boolean True Enforce this validation and fails records that do not pass
    *   with an error message False Enforces validation but logs warnings which
    *   will not fail a record.
    * @param collector
    *   MessageCollector Ingest message collector
    * @return
    *   String Returns mapped String or an empty String if none mapped
    */
  def validateOriginalId(
      values: ZeroToOne[String],
      providerId: String,
      enforce: Boolean
  )(implicit collector: MessageCollector[IngestMessage]): String = {
    values match {
      case Some(originalId) => originalId
      case None =>
        collector.add(
          missingRequiredFieldMsg(providerId, "originalId", enforce)
        )
        ""
    }
  }

  /** Performs validation check against DplaUri If the value is not a mintable
    * URI, a message is logged If no value was provided, a blank URI is return
    * and a message is logged
    *
    * @param values
    *   Option[URI] Dpla URI value generated during mapping
    * @param providerId
    *   String Local provider identifier
    * @param enforce
    *   Boolean True Enforce this validation and fails records that do not pass
    *   with an error message False Enforces validation but logs warnings which
    *   will not fail a record.
    * @param collector
    *   MessageCollector Ingest message collector
    * @return
    *   Returns mapped URI or an empty URI if none mapped
    */
  def validateDplaUri(
      values: Option[URI],
      providerId: String,
      enforce: Boolean
  )(implicit collector: MessageCollector[IngestMessage]): URI = {
    values match {
      case Some(dplaUri) =>
        if (!dplaUri.validate) {
          // Not a valid URI
          collector.add(
            mintUriMsg(
              providerId,
              field = "dplaUri",
              dplaUri.toString,
              msg = None,
              enforce
            )
          )
          URI("")
        } else dplaUri
      case None =>
        // No URI
        collector.add(missingRequiredFieldMsg(providerId, "dplaUri", enforce))
        URI("")
    }
  }

  /** Performs validation check of data mapped to iiifManifest If the value is
    * not a IIIF manifest URL, a message is logged If more than one values was
    * provided, message is logged and all but the first value are dropped
    *
    * @param values
    *   Seq[URI] IIIF Manfiest URL(s)
    * @param providerId
    *   String Local provider identifier
    * @param enforce
    *   Boolean True Enforce this validation and fails records that do not pass
    *   with an error message False Enforces validation but logs warnings which
    *   will not fail a record.
    * @param collector
    *   MessageCollector Ingest message collector
    * @return
    *   Returns mapped URI or an empty URI if none mapped
    */
  def validateIIIFManifestUrl(
      values: Seq[URI],
      providerId: String,
      enforce: Boolean
  )(implicit collector: MessageCollector[IngestMessage]): Option[URI] = {
    if (values.size > 1)
      collector.add(
        moreThanOneValueMsg(
          providerId,
          "iiifManifest",
          values.toString(),
          enforce
        )
      )

    values.headOption

    // TODO Validate manifest URL format
  }
}

class XmlMapper extends Mapper[NodeSeq, XmlMapping] {
  override def map(
      document: Document[NodeSeq],
      mapping: Mapping[NodeSeq]
  ): OreAggregation = {

    implicit val msgCollector: MessageCollector[IngestMessage] =
      new MessageCollector[IngestMessage]
    val providerId = Try {
      mapping.sidecar(document) \\ "prehashId"
    } match {
      case Success(s) => s.extractOrElse[String]("Unknown")
      case Failure(_) => s"Fatal error - Missing required ID $document"
    }

    // Field validation
    val validatedDataProvider = validateDataProvider(
      mapping.dataProvider(document),
      providerId,
      mapping.enforceDataProvider
    )
    // steps for mapping edmRights
    // 1. Normalize all mapped edmRights values
    val normalizedEdmRights =
      normalizeEdmRights(mapping.edmRights(document), providerId)
    // 2. Validate normalized string is one of 5xx acceptable URIs, log message if not
    val validatedEdmRights = validateEdmRights(
      normalizedEdmRights,
      providerId,
      mapping.enforceEdmRights
    )

    val validatedIsShownAt = validateIsShownAt(
      mapping.isShownAt(document),
      providerId,
      mapping.enforceIsShownAt
    )
    val validatedObject = validateObject(
      mapping.`object`(document),
      providerId,
      mapping.enforceObject
    )
    val validatedPreview = validatePreview(
      mapping.preview(document),
      providerId,
      mapping.enforcePreview
    )
    val validatedTitle =
      validateTitle(mapping.title(document), providerId, mapping.enforceTitle)
    val validatedOriginalId = validateOriginalId(
      mapping.originalId(document),
      providerId,
      mapping.enforceOriginalId
    )
    val validatedDplaUri = validateDplaUri(
      mapping.dplaUri(document),
      providerId,
      mapping.enforceDplaUri
    )

    // MappingException may be thrown from the `rights` method in provider mapping
    // See GPO mapping
    try {
      validateRights(
        mapping.rights(document),
        validatedEdmRights,
        providerId,
        mapping.enforceRights
      )
    } catch {
      case _: MappingException => // do nothing, error will be logged when entire record mapping is attempted, below
    }

    // Recommended field validation
    val validatedCreator = validateRecommendedProperty(
      mapping.creator(document),
      "creator",
      providerId,
      mapping.enforceCreator
    )
    val validatedDate = validateRecommendedProperty(
      mapping.date(document),
      "date",
      providerId,
      mapping.enforceDate
    )
    val validatedDescription = validateRecommendedProperty(
      mapping.description(document),
      "description",
      providerId,
      mapping.enforceDescription
    )
    val validatedFormat = validateRecommendedProperty(
      mapping.format(document),
      "format",
      providerId,
      mapping.enforceFormat
    )
    val validatedLanguage = validateRecommendedProperty(
      mapping.language(document),
      "language",
      providerId,
      mapping.enforceLanguage
    )
    val validatedPlace = validateRecommendedProperty(
      mapping.place(document),
      "place",
      providerId,
      mapping.enforcePlace
    )
    val validatedPublisher = validateRecommendedProperty(
      mapping.publisher(document),
      "publisher",
      providerId,
      mapping.enforcePublisher
    )
    val validatedSubject = validateRecommendedProperty(
      mapping.subject(document),
      "subject",
      providerId,
      mapping.enforceSubject
    )
    val validatedType = validateRecommendedProperty(
      mapping.`type`(document),
      "type",
      providerId,
      mapping.enforceType
    )
    val validatedIIIFManifest = validateIIIFManifestUrl(
      mapping.iiifManifest(document),
      providerId,
      mapping.enforceIIIF
    )

    Try {
      OreAggregation(
        dplaUri = validatedDplaUri,
        dataProvider = validatedDataProvider,
        edmRights = validatedEdmRights,
        hasView = mapping.hasView(document),
        intermediateProvider = mapping.intermediateProvider(document),
        isShownAt = validatedIsShownAt,
        `object` = validatedObject, // full size image
        originalRecord = formatXml(document.get),
        preview = validatedPreview, // thumbnail
        provider = mapping.provider(document),
        sidecar = toJsonString(mapping.sidecar(document)),
        tags = mapping.tags(document),
        iiifManifest = validatedIIIFManifest,
        mediaMaster = mapping.mediaMaster(document),
        sourceResource = DplaSourceResource(
          alternateTitle = mapping.alternateTitle(document),
          collection = mapping.collection(document),
          contributor = mapping.contributor(document),
          creator = validatedCreator,
          date = validatedDate,
          description = validatedDescription,
          extent = mapping.extent(document),
          format = validatedFormat,
          genre = mapping.genre(document),
          identifier = mapping.identifier(document),
          language = validatedLanguage,
          place = validatedPlace,
          publisher = validatedPublisher,
          relation = mapping.relation(document),
          replacedBy = mapping.replacedBy(document),
          replaces = mapping.replaces(document),
          rights = mapping.rights(document),
          rightsHolder = mapping.rightsHolder(document),
          subject = validatedSubject,
          temporal = mapping.temporal(document),
          title = validatedTitle,
          `type` = validatedType
        ),
        originalId = validatedOriginalId,
        messages = msgCollector.getAll.toSeq
      )
    } match {
      case Success(oreAggregation) => oreAggregation
      case Failure(f) =>
        msgCollector.add(exception(providerId, f))
        // Return an empty oreAggregation that contains all the messages generated from failed mapping
        emptyOreAggregation.copy(messages = msgCollector.getAll.toSeq)
    }
  }
}

class JsonMapper extends Mapper[JValue, JsonMapping] {
  override def map(
      document: Document[JValue],
      mapping: Mapping[JValue]
  ): OreAggregation = {

    implicit val msgCollector: MessageCollector[IngestMessage] =
      new MessageCollector[IngestMessage]
    val providerId = (mapping.sidecar(document) \\ "prehashId")
      .extractOrElse[String]("Unknown")

    // Field validation
    val validatedDataProvider = validateDataProvider(
      mapping.dataProvider(document),
      providerId,
      mapping.enforceDataProvider
    )
    // Steps for mapping edmRights
    // 1. Normalize all mapped edmRights values
    val normalizedEdmRights =
      normalizeEdmRights(mapping.edmRights(document), providerId)
    // 2. Validate normalized string is one of 5xx acceptable URIs, log warning if not
    val validatedEdmRights = validateEdmRights(
      normalizedEdmRights,
      providerId,
      mapping.enforceEdmRights
    )

    val validatedIsShownAt = validateIsShownAt(
      mapping.isShownAt(document),
      providerId,
      mapping.enforceIsShownAt
    )
    val validatedObject = validateObject(
      mapping.`object`(document),
      providerId,
      mapping.enforceObject
    )
    val validatedPreview = validatePreview(
      mapping.preview(document),
      providerId,
      mapping.enforcePreview
    )
    val validatedTitle =
      validateTitle(mapping.title(document), providerId, mapping.enforceTitle)
    val validatedOriginalId = validateOriginalId(
      mapping.originalId(document),
      providerId,
      mapping.enforceOriginalId
    )
    val validatedDplaUri = validateDplaUri(
      mapping.dplaUri(document),
      providerId,
      mapping.enforceDplaUri
    )

    // MappingException may be thrown from the `rights` method in provider mapping
    // See GPO mapping
    try {
      validateRights(
        mapping.rights(document),
        validatedEdmRights,
        providerId,
        mapping.enforceRights
      )
    } catch {
      case _: MappingException => // do nothing, error will be logged when entire record mapping is attempted, below
    }

    // Recommended field validation
    val validatedCreator = validateRecommendedProperty(
      mapping.creator(document),
      "creator",
      providerId,
      mapping.enforceCreator
    )
    val validatedDate = validateRecommendedProperty(
      mapping.date(document),
      "date",
      providerId,
      mapping.enforceDate
    )
    val validatedDescription = validateRecommendedProperty(
      mapping.description(document),
      "description",
      providerId,
      mapping.enforceDescription
    )
    val validatedFormat = validateRecommendedProperty(
      mapping.format(document),
      "format",
      providerId,
      mapping.enforceFormat
    )
    val validatedLanguage = validateRecommendedProperty(
      mapping.language(document),
      "language",
      providerId,
      mapping.enforceLanguage
    )
    val validatedPlace = validateRecommendedProperty(
      mapping.place(document),
      "place",
      providerId,
      mapping.enforcePlace
    )
    val validatedPublisher = validateRecommendedProperty(
      mapping.publisher(document),
      "publisher",
      providerId,
      mapping.enforcePublisher
    )
    val validatedSubject = validateRecommendedProperty(
      mapping.subject(document),
      "subject",
      providerId,
      mapping.enforceSubject
    )
    val validatedType = validateRecommendedProperty(
      mapping.`type`(document),
      "type",
      providerId,
      mapping.enforceType
    )
    val validatedIIIFManifest = validateIIIFManifestUrl(
      mapping.iiifManifest(document),
      providerId,
      mapping.enforceIIIF
    )

    Try {
      OreAggregation(
        dplaUri = validatedDplaUri,
        dataProvider = validatedDataProvider,
        edmRights = validatedEdmRights,
        hasView = mapping.hasView(document),
        intermediateProvider = mapping.intermediateProvider(document),
        isShownAt = validatedIsShownAt,
        `object` = validatedObject, // full size image
        originalRecord = formatJson(document.get),
        preview = validatedPreview, // thumbnail
        provider = mapping.provider(document),
        sidecar = toJsonString(mapping.sidecar(document)),
        tags = mapping.tags(document),
        iiifManifest = validatedIIIFManifest,
        mediaMaster = mapping.mediaMaster(document),
        sourceResource = DplaSourceResource(
          alternateTitle = mapping.alternateTitle(document),
          collection = mapping.collection(document),
          contributor = mapping.contributor(document),
          creator = validatedCreator,
          date = validatedDate,
          description = validatedDescription,
          extent = mapping.extent(document),
          format = validatedFormat,
          genre = mapping.genre(document),
          identifier = mapping.identifier(document),
          language = validatedLanguage,
          place = validatedPlace,
          publisher = validatedPublisher,
          relation = mapping.relation(document),
          replacedBy = mapping.replacedBy(document),
          replaces = mapping.replaces(document),
          rights = mapping.rights(document),
          rightsHolder = mapping.rightsHolder(document),
          subject = validatedSubject,
          temporal = mapping.temporal(document),
          title = validatedTitle,
          `type` = validatedType
        ),
        originalId = validatedOriginalId,
        messages = msgCollector.getAll.toSeq
      )
    } match {
      case Success(oreAggregation) => oreAggregation
      case Failure(f) =>
        msgCollector.add(exception(providerId, f))
        // Return an empty oreAggregation that contains all the messages generated from failed mapping
        emptyOreAggregation.copy(messages = msgCollector.getAll.toSeq)
    }
  }
}
