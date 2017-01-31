package la.dp.ingestion3.harvesters

import org.scalatest.{FlatSpec, Matchers}

/**
  * Test class for Harvester
  */
class HarvesterTest extends FlatSpec with Matchers {
  /**
    * Primary goal is to test that IDs can be safely regenerated in ingestion3
    * and match those generated in ingestion1 and ingestion2
    */

  it should " generate an md5 hash from this id and no provider abbreviation " in {
    val id = "oai:libcollab.temple.edu:dplapa:TEMPLE_p15037coll3_63879"
    assert(Harvester.generateMd5(id) === "8f914d27735eb5bdae656b0762ac7c15")
  }

  it should " generate an md5 hash from provider id and a provider abbreviation" in {
    val id = "pa--oai:libcollab.temple.edu:dplapa:TEMPLE_p15037coll3_63879"
    val prov = "pa"
    assert(Harvester.generateMd5(id, prov) === "e0c5a9a7d5f12b70b397314995793a6e")
  }
}
