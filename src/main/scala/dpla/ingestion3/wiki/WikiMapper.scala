package dpla.ingestion3.wiki

import java.util.regex.Pattern

import dpla.ingestion3.mappers.utils.JsonExtractor
import dpla.ingestion3.model.DplaMapData.{ExactlyOne, ZeroToOne}
import dpla.ingestion3.model.{EdmWebResource, OreAggregation, URI}
import dpla.ingestion3.utils.FlatFileIO
import org.json4s.jackson.JsonMethods.parse

import scala.io.Source
import scala.util.{Failure, Success, Try}

case class WikiCriteria(
    dataProvider: Boolean,
    asset: Boolean,
    rights: Boolean,
    id: Boolean
)

case class Eligibility(
    partnerWiki: String,
    partnerEligible: Boolean,
    dataProviderWiki: String,
    dataProviderEligible: Boolean
)

object WikiUri {
  val baseWikiUri = "http://www.wikidata.org/entity/"
}

trait WikiMapper extends JsonExtractor {

  /** Standardized rightsstatment and creative commons URIs eligible for
    * Wikimedia upload NoC-US http://rightsstatements.org/vocab/NoC-US PDM
    * http://creativecommons.org/publicdomain/mark CC0
    * http://creativecommons.org/publicdomain/zero CC-BY
    * http://creativecommons.org/licenses/by CC-BY-SA
    * http://creativecommons.org/licenses/by-sa
    */
  protected val wikiEligibleRightsUris = Seq(
    "http://rightsstatements.org/vocab/NoC-US/",
    "http://creativecommons.org/publicdomain/mark/",
    "http://creativecommons.org/publicdomain/zero/",
    "http://creativecommons.org/licenses/by/",
    "http://creativecommons.org/licenses/by-sa/"
  )

  protected lazy val blockedIds: Set[String] = getEligibleIds(
    blockedIdsFileList
  )
  protected lazy val allowedIds: Set[String] = getEligibleIds(
    allowedIdsFileList
  )

  def getAllowedIds: Set[String] = allowedIds

  // Files to source blocked ids from
  private val blockedIdsFileList = Seq(
    "/wiki/ignore-nara.txt"
  )
  private val allowedIdsFileList = Seq(
    "/wiki/allow-nara.txt"
  )

  // Files to source from
  private val wikiFileList = Seq(
    "/wiki/institutions_v2.json"
  )

  lazy val wikiEntityEligibility: Seq[Eligibility] = getWikiEntityEligibility

  /** @param isShownAt
    * @return
    */
  def buildIIIFFromUrl(isShownAt: EdmWebResource): Option[URI] = {
//    We want to go from
//    http://www.ohiomemory.org/cdm/ref/collection/p16007coll33/id/126923
//    to
//    http://www.ohiomemory.org/iiif/info/p16007coll33/126923/manifest.json
//
//    ^(.*)/collection/(.*?)/id/(.*?)$  -> \1iiif/info/\2/\3/manifest.json
//    The first match group should catch only through the TLD, not the /cdm/ref/ or
//    whatever is that in between part of the URL before /collection/ (which should be discarded).

    // TODO Do contentDM instances all have /cdm/ in their path?
    val contentDMre = "(.*)(.*\\/cdm\\/.*collection\\/)(.*)(\\/id\\/)(.*$)"
    val uri = isShownAt.uri.toString

    val pattern = Pattern.compile(contentDMre)
    val matcher = pattern.matcher(uri)
    matcher.matches()

    Try {
      val domain: String = matcher.group(1)
      val collection: String = matcher.group(3)
      val id: String = matcher.group(5)
      Some(URI(s"$domain/iiif/info/$collection/$id/manifest.json"))
    } match {
      case Success(s: Option[URI]) => s
      case Failure(_)              => None
    }
  }

  /** Parse institutional JSON file and create a partner + dataProvider pairing
    * to determine Wikimedia eligibility
    * @return
    */
  private def getWikiEntityEligibility: Seq[Eligibility] = {
    wikiFileList.flatMap(file => {
      val fileContentString = Source
        .fromInputStream(getClass.getResourceAsStream(file))
        .getLines()
        .mkString
      val json = parse(fileContentString)

      extractKeys(json).flatMap(partner => {
        val partnerWikiId = extractString(json \ partner \ "Wikidata").get
        val partnerEligible =
          extractString(json \ partner \ "upload").get.toBoolean
        extractKeys(json \ partner \ "institutions")
          .map(dataProvider => {
            val dataProviderWikiId = extractString(
              json \ partner \ "institutions" \ dataProvider \ "Wikidata"
            ).get
            val dataProviderEligible = extractString(
              json \ partner \ "institutions" \ dataProvider \ "upload"
            ).get.toBoolean
            Eligibility(
              partnerWiki = s"${WikiUri.baseWikiUri}$partnerWikiId",
              partnerEligible = partnerEligible,
              dataProviderWiki = s"${WikiUri.baseWikiUri}$dataProviderWikiId",
              dataProviderEligible = dataProviderEligible
            )
          })
      })
    })
  }

  /** Evaluate whether the standardized rights value is in the list of approved
    * for Wikimedia
    *
    * @param edmRights
    *   Rights URI to evaluate
    * @return
    *   True if rights statement is in approved list False otherwise
    */
  def isRightsWikiEligible(edmRights: Option[URI]): Boolean = edmRights match {
    case None => false // no rights, not valid
    case Some(uri) =>
      wikiEligibleRightsUris.find(uri.toString.startsWith(_)) match {
        case Some(_) => true // rights in wikiEligibleRightsUri, valid
        case None    => false // rights not in wikiEligibleRightsUri, not valid
      }
  }

  /** Evaluate whether the data provider have an wikidata entity uri associated
    * with it
    *
    * @param partnerUri
    *   Option[URI]
    * @param dataProviderUri
    *   Option[URI]
    * @return
    *   True if at least one URI is a wikidata URI False otherwise
    */
  def institutionalEligibility(
      partnerUri: ZeroToOne[URI],
      dataProviderUri: ZeroToOne[URI]
  ): Boolean = {
    (partnerUri, dataProviderUri) match {
      case (Some(partnerWikiUri), Some(dataProviderWikiUri)) =>
        wikiEntityEligibility.find(eligible =>
          // Compare list of defined eligibility in institutions file
          eligible.partnerWiki == partnerWikiUri.toString && eligible.dataProviderWiki == dataProviderWikiUri.toString
        ) match {
          // 1. True for "upload" field at the partner/hub-level signifies all eligible records from
          //    the hub can be uploaded. (disregard "upload" value at institution-level)
          // 2. False at hub-level means only upload the institutions which are marked true on the institution level
          case Some(e) => e.partnerEligible | e.dataProviderEligible
          case None    => false
        }
      case (_, _) => false
    }
  }

  def isWikiUri(uri: URI): Boolean =
    uri.toString.startsWith(WikiUri.baseWikiUri)

  /** Evaluate whether the combination of mediaMaster and iiifManifest values
    * make the record eligible for Wikimedia.
    *
    * @param iiif
    *   IIIF Manfiest URI
    * @param mediaMasters
    *   MediaMaster URIs
    * @return
    *   True if either exist False otherwise
    */
  def isAssetEligible(
      iiif: Option[URI],
      mediaMasters: Seq[EdmWebResource]
  ): Boolean =
    (iiif, mediaMasters.nonEmpty) match {
      case (None, false) =>
        false // Neither IIIF manifest nor media masters exist
      case (_, _) => true // either IIIF manifest or exactly one media exist
    }

  /** Evaluate whether the DPLA ID belongs to the set of IDs which should be
    * excluded from being uploaded to Wikimedia
    * @param id
    *   DPLA ID
    * @return
    *   True if id is *not* in block list False if id is in block list
    */
  def isIdEligible(id: ExactlyOne[String]): Boolean =
    allowedIds.contains(id) | !blockedIds.contains(id)

  /** Evaluate all eligibility checks, dataProvider, rights, asset, id
    *
    * @param record
    *   DPLA record to evaluate
    * @return
    *   WikiCriteria
    */
  def isWikiEligible(record: OreAggregation): WikiCriteria = {
    val dataProvider = institutionalEligibility(
      record.provider.exactMatch.find(isWikiUri),
      record.dataProvider.exactMatch.find(isWikiUri)
    )

    val rights = isRightsWikiEligible(record.edmRights)
    val asset = isAssetEligible(record.iiifManifest, record.mediaMaster)
    val id = isIdEligible(record.originalId)

    WikiCriteria(dataProvider, asset, rights, id)
  }

  /** Get the list of blocked IDs from file
    */
  def getBlockedIds: Set[String] = {
    val io = new FlatFileIO()
    blockedIdsFileList.flatMap(file => io.readFileAsSeq(file)).toSet
  }

  def getEligibleIds(fileList: Seq[String]): Set[String] = {
    val io = new FlatFileIO()
    fileList.flatMap(file => io.readFileAsSeq(file)).toSet
  }
}
