package dpla.ingestion3.wiki

import dpla.ingestion3.model._
import org.scalatest.{FlatSpec, Matchers}

/**
  * Tests for validating wikimedia eligibility  
  */
class MapperTest extends WikiMapper

class WikiMapperTest extends FlatSpec with Matchers {

  val wiki = new MapperTest

  val oreAggregation: OreAggregation = emptyOreAggregation.copy(
    provider = EdmAgent(
      uri = Some(URI("http://dp.la/api/contributor/indiana")),
      name = Some("Indiana Memory"),
      exactMatch = List(URI("http://www.wikidata.org/entity/Q83878471"))
    ),
    dataProvider = EdmAgent(
        uri = Some(URI("http://dp.la/api/contributor/benjamin-harrison-presidential-site")),
        name = Some("Benjamin Harrison Presidential Site"),
        exactMatch = List(URI("http://www.wikidata.org/entity/Q4888783"))
      ),
    edmRights = Some(URI("http://rightsstatements.org/vocab/NoC-US/1.0/")),
    isShownAt = stringOnlyWebResource("http://indianamemory.contentdm.oclc.org/cdm/ref/collection/BHPS/id/6341"),
    iiifManifest = Some(URI("https://indianamemory.contentdm.oclc.org/cdm/collection/p16066coll13/id/71"))
  )

  "isRightsWikiEligible" should " return true if edmRights is valid URI" in {
    val rightsUri = Some(URI("http://rightsstatements.org/vocab/NoC-US/1.0/"))
    assert(wiki.isRightsWikiEligible(rightsUri) === true)
  }

  it should " return false if edmRights is not a valid URI" in {
    val rightsUri = Some(URI("http://rightsstatements.org/page/NoC-US/1.0/"))
    assert(wiki.isRightsWikiEligible(rightsUri) === false)
  }

  "institutionalEligibility" should " return true if the partner is Wiki eligible and dataProvider is not" in {
    val partnerUri = Some(URI(s"${wiki.baseWikiUri}Q518155")) // nara
    val dataProviderUri = Some(URI(s"${wiki.baseWikiUri}Q59661289")) // obama library
    assert(wiki.institutionalEligibility(partnerUri, dataProviderUri) === true)
  }

  it should " return `true` if a partner is not eligible but a dataProvider is " in {
    val partnerUri = Some(URI(s"${wiki.baseWikiUri}Q5275908")) // DLG
    val dataProviderUri = Some(URI(s"${wiki.baseWikiUri}Q30267984")) // Augusta-Richmond County Public Library
    assert(wiki.institutionalEligibility(partnerUri, dataProviderUri) === true)
  }

  it should " return `false` if neither partner nor dataProvider is eligible" in {
    val partnerUri = Some(URI(s"${wiki.baseWikiUri}Q5275908")) // DLG
    val dataProviderUri = Some(URI(s"${wiki.baseWikiUri}Q4815975")) // Atlanta-Fulton Public Library System
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

  "buildIIIFFromUrl" should "build a IIIF manfiest from ContentDM URL" in {
    val isShownAt = stringOnlyWebResource("http://www.ohiomemory.org/cdm/ref/collection/p16007coll33/id/126923")
    val expected = Some(URI("http://www.ohiomemory.org/iiif/info/p16007coll33/126923/manifest.json"))
    assert(wiki.buildIIIFFromUrl(isShownAt) === expected)
  }

  it should "not build a IIIF manfiest from a non-ContentDM URL" in {
    val isShownAt = stringOnlyWebResource("https://digitalgallery.bgsu.edu/collections/item/8911")
    val expected = None
    assert(wiki.buildIIIFFromUrl(isShownAt) === expected)
  }

  it should "validate this record" in {
    assert(wiki.isWikiEligible(oreAggregation) === WikiCriteria(
      dataProvider = true,
      asset = true,
      rights = true,
      id = true)
    )
  }
}