package dpla.ingestion3.enrichments

// JavaConversions is for iterating over Sets of
// org.eclipse.rdf4j.model.Resource and org.eclipse.rdf4j.model.Model
import com.github.dvdme.Coordinates
import dpla.ingestion3.model.DplaPlace
import dpla.ingestion3.utils.HttpUtils
import org.apache.http.client.utils.URIBuilder
import org.apache.log4j.Logger
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.util.{Failure, Success, Try}


/* Case classes for JSON parser ... */
case class Coords(lat: Double, lng: Double)

case class Bounds(ne: Coords, sw: Coords)

case class Geometry(center: Coords, bounds: Bounds)

case class Feature(geometry: Geometry, woeType: Int, name: String,
                   displayName: String)

case class Parent(name: String, displayName: String, woeType: Int)

case class Interpretation(feature: Feature, parents: List[Parent])

case class Result(interpretations: List[Interpretation])


/**
  * Twofishes Geocoder trait
  *
  * Create an Object that extends this and pass it to the SpatialEnrichment
  * constructor when you create a spatial enrichment.
  *
  * @example
  * object Geocoder extends Twofisher {
  * override def hostname = {
  *              System.getenv("GEOCODER_HOST") match {
  * case h if h.isInstanceOf[String] => h
  * case _ => "localhost"
  * }
  * }
  * }
  * @see SpatialEnrichmentIntegrationTest
  */
trait Twofisher {

  /**
    * Return the JValue object from an HTTP request to Twofishes.
    *
    * @param queryType Type of query: "query" for name or "ll" for lat, long
    * @param term      The term that will be queried: coordinates or name
    * @return The response JValue, containing geographic
    *         interpretations
    */
  def geocoderResponse(queryType: String, term: String): JValue = {
   val url = new URIBuilder()
      .setScheme("http")
      .setHost(hostname)
      .setPort(new Integer(port).toInt)
      .setPath("/query")
      .addParameter("lang", "en")
      .addParameter("responseIncludes", "PARENTS,DISPLAY_NAME")
      .addParameter(queryType, term)
      .build()
      .toURL

    HttpUtils.makeGetRequest(url) match {
      case Success(body) => parse(body)
      case Failure(_) => JNothing
    }
  }

  /**
    * Retry method taken from http://bit.ly/2fdHVQb
    * credit to @leedm777
    */
  @annotation.tailrec
  final def retry[T](n: Int)(fn: => T): util.Try[T] = {
    util.Try { fn } match {
      case x: util.Success[T] => x
      case _ if n > 1 => retry(n - 1)(fn)
      case fn => fn
    }
  }

  def hostname: String = "localhost"

  def port: String = "8081"

  def getBaseUri: String = s"http://$hostname:$port/query"

}

/**
  * The DplaPlace spatial object enrichment
  *
  * @param geocoder A singleton object representing your geocoder
  */
class SpatialEnrichment(geocoder: Twofisher) extends Serializable {

  implicit val formats = DefaultFormats // Formats for json4s
  val logger: Logger = Logger.getLogger(getClass.getName)

  /**
    * Enrich the given DplaPlace
    *
    * @param place The Place
    * @return An enriched copy of the Place
    */
  def enrich(place: DplaPlace): DplaPlace = {
    queryTerm(place) match {
      case Some(term) =>
        val coordsTerm = coordinatesFromString(term)
        val jval = coordsTerm match {
          case Some(value) =>
            geocoder.geocoderResponse("ll", value)
          case None =>
            geocoder.geocoderResponse("query", term)
        }
        // Return the looked-up place, unless it was a country.
        woeType(jval) match {
          case Success(wt) =>
            if (wt != 12) {
              place.copy(
                name = realPlaceName(place.name, jval, coordsTerm.isDefined),
                city = place.city.orElse(city(jval)),
                county = place.county.orElse(county(jval)),
                state = place.state.orElse(state(jval)),
                country = place.country.orElse(country(jval)),
                coordinates = place.coordinates.orElse(coordinates(jval))
              )
            } else {
              place
            }
          case Failure(_) => place
        }
      case _ => place
    }
  }

  /**
    * Given a Place that needs amending, Return the query term string that will
    * be passed to Twofishes, depending on whether the coordinates or name are
    * already filled in.
    *
    * Return nothing if the place is the United States. We don't want to resolve
    * "United States" unless it's something like "United States--California"
    * with two dashes, because we'd get too many results otherwise that are at
    * the geographic center of the U.S., in Kansas.  Note that we'll skip
    * lookups on all countries in enrich(), but this saves some processing time,
    * avoiding unnecessary lookups in the first place, because of the volume of
    * Places that we expect to be given as "United States."
    *
    * @param place The edm:Place object to consider
    * @return The query term: either the name or coordinates, or None
    */
  private def queryTerm(place: DplaPlace): Option[String] = place match {
    case DplaPlace(_, _, _, _, _, _, Some(coordinates), _) =>
      Some(coordinates)
    case DplaPlace(Some(name), _, _, _, _, _, _, _) =>
      name match {
        case s if s matches """^United States(?!\-\-)""" => None
        case s if s matches "^USA" => None
        case _ => Some(name)
      }
    case _ =>
      None
  }

  /**
    * Return the city / town name of the Place.
    *
    * Note that this is not handled by `parentFeatureName` because the City name is
    * going to be the name of the place, City / Town being the smallest level
    * of detail reported by Twofishes. It's not going to be a parent feature.
    *
    * @param j Twofishes response body JValue
    * @return The city / town name
    */
  private def city(j: JValue): Option[String] = {
    val feature = ((j \ "interpretations") (0) \ "feature").extract[Feature]
    if (feature.woeType == 7) {
      Option(feature.name)
    } else {
      None
    }
  }

  /* Similar to `city`, return the other parent feature names ... */
  private def county(j: JValue): Option[String] = {
    if (woeType(j).getOrElse(0) == 9) shortName(j)
    else parentFeatureName(j, 9)
  }

  /**
    * Return the place's short name ('name' field)
    *
    * @param j The JValue object for the Twofishes response body
    * @return The place's short name
    */
  private def shortName(j: JValue): Option[String] = {
    Option(((j \ "interpretations") (0) \ "feature")
      .extract[Feature]
      .name)
  }

  /**
    * Return the What-on-Earth type of the feature
    *
    * For decision-making about whether to return it with coordinates.
    *
    * Returns a Try because it may be handed a JValue from an unsuccessful
    * lookup, meaning there will be no woeType defined and the JSON parsing
    * will fail on the woeType property.
    *
    * @see enrich()
    * @param j The JValue object for the Twofishes response body
    * @return The woeType
    */
  private def woeType(j: JValue): Try[Int] = Try {
    ((j \ "interpretations") (0) \ "feature")
      .extract[Feature]
      .woeType
  }

  /**
    * Return a parent feature's name, given the Twofishes response and a WoeType
    * (What-on-Earth Type) for the rank of the feature (city, state, etc.).
    *
    * @param json          Twofishes response body JValue
    * @param filterWoeType What-on-Earth Type number
    * @return
    */
  private def parentFeatureName(json: JValue, filterWoeType: Int):
  Option[String] = {
    val featNames: List[String] = for {
      JArray(parents) <- json \\ "parents"
      JObject(parent) <- parents
      JField("name", JString(name)) <- parent
      JField("woeType", JInt(wt)) <- parent
      if wt == filterWoeType
    } yield name
    if (featNames.nonEmpty) Option(featNames.head)
    else None
  }

  private def state(j: JValue): Option[String] = {
    if (woeType(j).getOrElse(0) == 8) shortName(j)
    else parentFeatureName(j, 8)
  }

  private def country(j: JValue): Option[String] = {
    if (woeType(j).getOrElse(0) == 12) shortName(j)
    else parentFeatureName(j, 12)
  }

  // Region is implemented inconsistently in the Ingestion1 Twofishes enricher,
  // and Twofishes does not return it consistently. If you look up "United
  // States" it will provide the continent, "North America", but if you look up
  // a city in the United States it will not return the continent.  Filling in
  // a region value will result in misleading search results.
  // private def region(j: JValue): String = ???

  /**
    * Return a single String of the Place's coordinates, joined by ","
    *
    * @param j Twofishes response JValue
    * @return The coordinates as at "lat, lng" string
    */
  private def coordinates(j: JValue): Option[String] = {
    val c = ((((j \ "interpretations") (0) \ "feature") \ "geometry") \
      "center").extract[Coords]
    Some(c.lat.toString + "," + c.lng.toString)
  }

  /**
    * Given a candidate place name and some Twofishes feature data, return
    * the place name if it's not empty. If it's being used to convey
    * coordinates, look up a better place name from the Twofishes data.
    * Return nothing if the name wasn't filled in in the first place.
    *
    * The point is not to clobber a name that's already been filled in by the
    * provider, but to improve it if it was just being used for coordinates.
    *
    * @see enrich()
    * @param nameCandidate The string to consider
    * @param jval          The Twofishes feature data
    * @param nameIsCoords  Whether the string represents coordinates
    */
  private def realPlaceName(nameCandidate: Option[String],
                            jval: JValue,
                            nameIsCoords: Boolean): Option[String] = {
    nameCandidate match {
      case n if n.isDefined =>
        if (!nameIsCoords) n // Keep 'name' as-is
        else displayName(jval) // Replace 'name' with a place name
      case _ => None // There's no place name yet.
    }
  }

  /**
    * Return the place's display name, for sourceResource.spatial.name
    *
    * @param j The JValue object for the Twofishes response body
    * @return The place's display name
    */
  private def displayName(j: JValue): Option[String] = {
    Option(((j \ "interpretations") (0) \ "feature")
      .extract[Feature]
      .displayName)
  }

  /**
    * Given a place name string, return a decimal coordinates string suitable
    * for Twofishes, or None
    *
    * @param s The candidate string
    * @return A comma-separated decimal coordinates string
    */
  private def coordinatesFromString(s: String): Option[String] = {
    val parts: Array[String] = s.split(",")
    parts.length match {
      case 2 => Try {
        val c: Coordinates = new Coordinates(parts(0), parts(1))
        Option(c.getLatitudeAsString + "," + c.getLongitudeAsString)
      }.getOrElse(None)
      case _ => None
    }

  }

}
