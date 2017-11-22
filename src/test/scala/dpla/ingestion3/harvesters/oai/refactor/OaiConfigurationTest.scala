package dpla.ingestion3.harvesters.oai.refactor

import org.scalatest.{FlatSpec, FunSuite}

class OaiConfigurationTest extends FlatSpec {

  val defaults = Map("verb" -> "ListRecords")

  "An OaiConfiguration" should "parse out the endpoint from parameters" in {
    val endpoint = "http://www.google.com"
    val config = OaiConfiguration(defaults.updated("endpoint", endpoint))
    assert(config.endpoint === endpoint)
  }

  it should "recognize when an endpoint is not reachable" in {
    val endpoint = "http://www.ffdf123.com"
    val config = OaiConfiguration(defaults.updated("endpoint", endpoint))
    assertThrows[Exception](config.endpoint)
  }

  it should "throw when no endpoint is given" in {
    val config = OaiConfiguration(defaults)
    assertThrows[Exception](config.endpoint)
  }

  it should "parse out an oai verb" in {
    val verb = "ListRecords"
    val config = OaiConfiguration(Map("verb" -> verb))
    assert(config.verb === verb)
  }

  it should "detect when an invalid verb is passed" in {
    val verb = "foop"
    assertThrows[Exception](OaiConfiguration(Map("verb" -> verb)))
  }

  it should "throw when no verb is passed" in {
    assertThrows[Exception](OaiConfiguration(Map()))
  }

  it should "parse out the metadataPrefix" in {
    val metadataPrefix = "CLODS"
    val config = OaiConfiguration(defaults.updated("metadataPrefix", metadataPrefix))
    assert(config.metadataPrefix === Some(metadataPrefix))
  }

  it should "determine whether to harvest all sets" in {
    assert(OaiConfiguration(defaults.updated("harvestAllSets", "true")).harvestAllSets === true)
    assert(OaiConfiguration(defaults.updated("harvestAllSets", "false")).harvestAllSets === false)
    assert(OaiConfiguration(defaults).harvestAllSets === false)
    assertThrows[Exception](OaiConfiguration(defaults.updated("harvestAllSets", "boing")).harvestAllSets)
  }

  it should "parse a set list" in {
    val sets = Seq("a","b","c","d")
    val config = OaiConfiguration(defaults.updated("setlist", sets.mkString(",")))
    assert(config.setlist.getOrElse(Array()) === sets)
  }

  it should "parse a blacklist" in {
    val sets = Seq("a","b","c","d")
    val config = OaiConfiguration(defaults.updated("blacklist", sets.mkString(",")))
    assert(config.blacklist.getOrElse(Array()) === sets)
  }

  it should "throw when ListRecords is not set and a set parameter is also not set" in {
    assertThrows[Exception](OaiConfiguration(Map()))
  }

}
