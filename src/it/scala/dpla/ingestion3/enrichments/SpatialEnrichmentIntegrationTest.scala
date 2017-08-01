package dpla.ingestion3.enrichments

import dpla.ingestion3.model.DplaPlace
import org.scalatest.FlatSpec


object TestGeocoder extends Twofisher {
  override def hostname: String = {
    System.getenv("GEOCODER_HOST") match {
      case h if h.isInstanceOf[String] => h
      case _ => "localhost"
    }
  }
}


class SpatialEnrichmentIntegrationTest extends FlatSpec {
  val e = new SpatialEnrichment(TestGeocoder)

  "Spatial.enrich" should "perform lookup and return a copy DplaPlace" in {
    val origPlace = DplaPlace(name=Option("Somerville, MA"))
    val newPlace = e.enrich(origPlace)
    assert(newPlace.isInstanceOf[DplaPlace])
    assert(newPlace != origPlace)
  }

  it should "work with dotted state abbreviations" in {
    val origPlace = DplaPlace(name=Option("Greenville (S.C.)"))
    val newPlace = e.enrich(origPlace)
    assert(newPlace.state.mkString == "South Carolina")
  }

  it should "return the original place if it can't be resolved" in {
    val origPlace = DplaPlace(name=Option("Palookaville"))
    val newPlace = e.enrich(origPlace)
    assert(origPlace == newPlace)
  }

}
