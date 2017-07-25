package dpla.ingestion3.harvesters.oai

import org.scalatest._

/**
  * Tests for DefaultSourceTest
  */
class DefaultSourceTest extends FlatSpec {
  val source = new DefaultSource
  val endpoint = "http://google.com" // TODO replace with HTTP simulation
  val verb = "ListRecords"
  val list = "manuscripts, newspapers, maps"
  val prefix = "mods"
  val token = "1234"

  "getEndpoint" should "throw Exception if endpoint missing" in {
    val params = Map("foo" -> "bar")
    assertThrows[Exception](source.getEndpoint(params))
  }
  // TODO rewrite with HTTP simulation
  it should "return endpoint as String if the endpoint is provided and reachable" in {
    val params = Map("endpoint" -> endpoint)
    assert(source.getEndpoint(params) === endpoint)
  }

  "getVerb" should "throw Exception if verb missing" in {
    val params = Map("foo" -> "bar")
    assertThrows[Exception](source.getVerb(params))
  }
  it should "return verb as String" in {
    val params = Map("verb" -> verb)
    assert(source.getVerb(params) === verb)
  }

  "getHarvestAllRecords" should "return false if harvestAllSets missing" in {
    val params = Map("foo" -> "bar")
    assert(source.getHarvestAllSets(params) === false)
  }
  it should "return true if harvestAllSets is true" in {
    val params = Map("harvestAllSets" -> "true")
    assert(source.getHarvestAllSets(params) === true)
  }
  it should "return false if harvestAllSets is false" in {
    val params = Map("harvestAllSets" -> "false")
    assert(source.getHarvestAllSets(params) === false)
  }
  it should "ignore case" in {
    val params = Map("harvestAllSets" -> "TRUE")
    assert(source.getHarvestAllSets(params) === true)
  }
  it should "throw Exception if harvestAllSets is neither true nor false" in {
    val params = Map("harvestAllSets" -> "foo")
    assertThrows[Exception](source.getHarvestAllSets(params))
  }

  "getSetlist" should "return None if setlist missing" in {
    val params = Map("foo" -> "bar")
    assert(source.getSetlist(params) === None)
  }
  it should "return set list as Some[Array[String]]" in {
    val params = Map("setlist" -> list)
    val sourceOption = source.getSetlist(params).getOrElse(Array())
    assert(sourceOption.deep === Array("manuscripts", "newspapers", "maps").deep)
  }

  "getBlacklist" should "return None if blacklist missing" in {
    val params = Map("foo" -> "bar")
    assert(source.getBlacklist(params) === None)
  }
  it should "return set list as Some[Array[String]]" in {
    val params = Map("blacklist" -> list)
    val sourceOption = source.getBlacklist(params).getOrElse(Array())
    assert(sourceOption.deep === Array("manuscripts", "newspapers", "maps").deep)
  }
}
