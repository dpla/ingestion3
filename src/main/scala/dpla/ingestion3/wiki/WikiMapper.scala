package dpla.ingestion3.wiki

import dpla.ingestion3.model.DplaMapData.ExactlyOne
import dpla.ingestion3.model.{EdmWebResource, OreAggregation, URI}
import dpla.ingestion3.utils.FlatFileIO

case class WikiCriteria(dataProvider: Boolean, asset: Boolean, rights: Boolean, id: Boolean)

trait WikiMapper {

  /**
    * Standardized rightsstatment and creative commons URIs eligible for Wikimedia upload
    *  NoC-US   http://rightsstatements.org/vocab/NoC-US
    *  PDM      http://creativecommons.org/publicdomain/mark
    *  CC0      http://creativecommons.org/publicdomain/zero
    *  CC-BY    http://creativecommons.org/licenses/by
    *  CC-BY-SA http://creativecommons.org/licenses/by-sa
    */
  protected val wikiEligibleRightsUris = Seq(
    "http://rightsstatements.org/vocab/NoC-US/",
    "http://creativecommons.org/publicdomain/mark/",
    "http://creativecommons.org/publicdomain/zero/",
    "http://creativecommons.org/licenses/by/",
    "http://creativecommons.org/licenses/by-sa/")

  protected lazy val blockedIds: Set[String] = getBlockedIds

  // Files to source blocked ids from
  private val blockedIdsFileList = Seq(
    "/wiki/ignore-nara.txt"
  )

  /**
    * Evaluate whether the standardized rights value is in the list of approved for Wikimedia
    *
    * @param edmRights Rights URI to evaluate
    * @return True if rights statement is in approved list
    *         False otherwise
    */
  def isRightsWikiEligible(edmRights: Option[URI]): Boolean = edmRights match {
    case None => false // no rights, not valid
    case Some(uri) => wikiEligibleRightsUris.find(uri.toString.startsWith(_)) match {
        case Some(_) => true // rights in wikiEligibleRightsUri, valid
        case None => false // rights not in wikiEligibleRightsUri, not valid
      }
    }

  /**
    * Evaluate whether the data provider have an wikidata entity uri associated with it
    *
    * @param uris URIs to evaluate
    * @return True if at least one URI is a wikidata URI
    *         False otherwise
    */
  def isDataProviderWikiEligible(uris: Seq[URI]): Boolean =
    uris.find(uri => uri.toString.startsWith("https://wikidata.org/wiki/")) match {
      case Some(_) => true
      case None => false
    }

  /**
    * Evaluate whether the combination of mediaMaster and iiifManifest values make the record eligible for Wikimedia.
    *
    * @param iiif IIIF Manfiest URI
    * @param mediaMasters MediaMaster URIs
    * @return True if either exist
    *         False otherwise
    */
  def isAssetEligible(iiif: Option[URI], mediaMasters: Seq[EdmWebResource]): Boolean =
    (iiif, mediaMasters.size.equals(1) && mediaMasters.head.uri.value.toLowerCase.contains(".jp")) match {
      case(None, false) => false // Neither IIIF manifest nor media masters exist
      case(_, _) => true // either IIIF manifest or exactly one media exist
    }

  /**
    * Evaluate whether the DPLA ID belongs to the set of IDS which should be excluded from being uploaded to Wikimeida
    * @param id DPLA ID
    * @return True if id is *not* in block list
    *         False if id is in block list
    */
  def isIdEligible(id: ExactlyOne[String]): Boolean = !blockedIds.contains(id)

  /**
    * Evaluate all eligibility checks, dataProvider, rights, asset, id
    *
    * @param record DPLA record to evaluate
    * @return WikiCriteria
    */
  def isWikiEligible(record: OreAggregation): WikiCriteria = {
    val dataProvider = isDataProviderWikiEligible(record.dataProvider.exactMatch)
    val rights = isRightsWikiEligible(record.edmRights)
    val asset = isAssetEligible(record.iiifManifest, record.mediaMaster)
    val id = isIdEligible(record.originalId)

    WikiCriteria(dataProvider, asset, rights, id)
  }

  /**
    * Get the list of blocked IDs from file
    */
  def getBlockedIds: Set[String] = {
    val io = new FlatFileIO()
    blockedIdsFileList.flatMap(file => io.readFileAsSeq(file)).toSet
  }
}
