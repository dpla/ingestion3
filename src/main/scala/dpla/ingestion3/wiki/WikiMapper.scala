package dpla.ingestion3.wiki

import dpla.ingestion3.model.{EdmWebResource, OreAggregation, URI}

case class WikiCriteria(dataProvider: Boolean, asset: Boolean, rights: Boolean)

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

  /**
    *
    * @param edmRights
    * @return
    */
  def isRightsWikiEligible(edmRights: Option[URI]): Boolean = edmRights match {
      case Some(uri) => wikiEligibleRightsUris.find(uri.toString.startsWith(_)) match {
        case Some(_) => true
        case None => false
      }
      case None => false
    }

  /**
    *
    * @param uris
    * @return
    */
  def isDataProviderWikiEligible(uris: Seq[URI]): Boolean =
    uris.find(uri => uri.toString.startsWith("https://wikidata.org/wiki/")) match {
      case Some(_) => true
      case None => false
    }

  def isAssetEligible(iiif: Option[URI], mediaMasters: Seq[EdmWebResource]): Boolean =
    (iiif, mediaMasters.isEmpty) match {
      case(None, true) => false // IIIF manifest and media masters do not exist
      case(_, _) => true // either IIIF manifest or media exist
    }
  /**
    *
    * @param record
    * @return
    */
  def isWikiEligible(record: OreAggregation) = {
    val dataProvider = isDataProviderWikiEligible(record.dataProvider.exactMatch)
    val rights = isRightsWikiEligible(record.edmRights)
    val asset = isAssetEligible(record.iiifManifest, record.mediaMaster)

//    if(dataProvider && asset)
//      println(s"$dataProvider && $asset && $rights == ${record.edmRights}")

    WikiCriteria(dataProvider, asset, rights)
  }
}
