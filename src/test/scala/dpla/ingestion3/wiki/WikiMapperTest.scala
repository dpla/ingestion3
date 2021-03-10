package dpla.ingestion3.wiki

import dpla.ingestion3.model._
import org.scalatest.{FlatSpec, Matchers}

/**
  * Tests for validating wikimedia eligibility  
  */
class MapperTest extends WikiMapper

class WikiMapperTest extends FlatSpec with Matchers {

  val wiki = new MapperTest

  "isRightsWikiEligible" should " return true if edmRights is valid URI" in {
    val rightsUri = Some(URI("http://rightsstatements.org/vocab/NoC-US/1.0/"))
    assert(wiki.isRightsWikiEligible(rightsUri) === true)
  }

  it should " return false if edmRights is not a valid URI" in {
    val rightsUri = Some(URI("http://rightsstatements.org/page/NoC-US/1.0/"))
    assert(wiki.isRightsWikiEligible(rightsUri) === false)
  }

  "institutionalEligibility" should " return true if the partner is Wiki eligible and dataProvider is not" in {
    val partnerUri = Some(URI("https://wikidata.org/wiki/Q518155")) // nara
    val dataProviderUri = Some(URI("https://wikidata.org/wiki/Q59661289")) // obama library
    assert(wiki.institutionalEligibility(partnerUri, dataProviderUri) === true)
  }

  it should " return `true` if a partner is not eligible but a dataProvider is " in {
    val partnerUri = Some(URI("https://wikidata.org/wiki/Q5275908")) // DLG
    val dataProviderUri = Some(URI("https://wikidata.org/wiki/Q30267984")) // Augusta-Richmond County Public Library
    assert(wiki.institutionalEligibility(partnerUri, dataProviderUri) === true)
  }

  it should " return `false` if neither partner nor dataProvider is eligible" in {
    val partnerUri = Some(URI("https://wikidata.org/wiki/Q5275908")) // DLG
    val dataProviderUri = Some(URI("https://wikidata.org/wiki/Q4815975")) // Atlanta-Fulton Public Library System
    assert(wiki.institutionalEligibility(partnerUri, dataProviderUri) === false)
  }

  "isAssetEligible" should "return true if both IIIF manifest and media master given" in {
    val iiif = Some(URI("http://iiif.manifest"))
    val mediaMasters = Seq(stringOnlyWebResource("http://media.master.com/image.jpg"))

    assert(wiki.isAssetEligible(iiif, mediaMasters) === true)
  }
  it should "return true if only IIIF manifest exists " in {
    val iiif = Some(URI("http://iiif.manifest"))
    val mediaMasters = Seq() //Seq("http://media.master").map(stringOnlyWebResource)

    assert(wiki.isAssetEligible(iiif, mediaMasters) === true)
  }
  it should "return true if only media master exists " in {
    val iiif = None // Some(URI("http://iiif.manifest"))
    val mediaMasters = Seq("http://media.master/image.jpg").map(stringOnlyWebResource)

    assert(wiki.isAssetEligible(iiif, mediaMasters) === true)
  }
  it should "return false if neither IIIF manfiest nor media master exists " in {
    val iiif = None // Some(URI("http://iiif.manifest"))
    val mediaMasters = Seq() // Seq("http://media.master").map(stringOnlyWebResource)

    assert(wiki.isAssetEligible(iiif, mediaMasters) === false)
  }
}