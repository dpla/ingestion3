package dpla.ingestion3.wiki

import dpla.ingestion3.data.EnrichedRecordFixture
import dpla.ingestion3.model.{URI, _}
import org.scalatest.{FlatSpec, Matchers}

/**
  * Tests for the ingestion3 FileIO utility
  */
class WikiMapperTest extends FlatSpec with Matchers {

  class MapperTest extends WikiMapper

  val wiki = new MapperTest

  "isRightsWikiEligible" should " return true if edmRights is valid URI" in {
    val rightsUri = Some(URI("http://rightsstatements.org/vocab/NoC-US/1.0/"))
    assert(wiki.isRightsWikiEligible(rightsUri) === true)
  }

  it should " return false if edmRights is not a valid URI" in {
    val rightsUri = Some(URI("http://rightsstatements.org/page/NoC-US/1.0/"))
    assert(wiki.isRightsWikiEligible(rightsUri) === false)
  }

  "isDataProviderEligible" should " return true if there is a wiki URI" in {
    val uris = Seq(URI("https://wikidata.org/wiki/Q324534"))
    assert(wiki.isDataProviderWikiEligible(uris) === true)
  }

  it should " return true if there is a wiki URI and a non-wiki URI" in {
    val uris = Seq(URI("https://viaf.org/id/2342123"), URI("https://wikidata.org/wiki/Q324534"))
    assert(wiki.isDataProviderWikiEligible(uris) === true)
  }

  it should " return false if there is no wiki uri" in {
    val uris = Seq(URI("https://viaf.org/id/2342123"))
    assert(wiki.isDataProviderWikiEligible(uris) === false)
  }

  "isAssetEligible" should "return true if both IIIF manifest and media master given" in {
    val iiif = Some(URI("http://iiif.manifest"))
    val mediaMasters = Seq("http://media.master").map(stringOnlyWebResource)

    assert(wiki.isAssetEligible(iiif, mediaMasters) === true)
  }
  it should "return true if only IIIF manifest exists " in {
    val iiif = Some(URI("http://iiif.manifest"))
    val mediaMasters = Seq() //Seq("http://media.master").map(stringOnlyWebResource)

    assert(wiki.isAssetEligible(iiif, mediaMasters) === true)
  }
  it should "return true if only media master exists " in {
    val iiif = None // Some(URI("http://iiif.manifest"))
    val mediaMasters = Seq("http://media.master").map(stringOnlyWebResource)

    assert(wiki.isAssetEligible(iiif, mediaMasters) === true)
  }
  it should "return false if neither IIIF manfiest nor media master exists " in {
    val iiif = None // Some(URI("http://iiif.manifest"))
    val mediaMasters = Seq() // Seq("http://media.master").map(stringOnlyWebResource)

    assert(wiki.isAssetEligible(iiif, mediaMasters) === false)
  }
}