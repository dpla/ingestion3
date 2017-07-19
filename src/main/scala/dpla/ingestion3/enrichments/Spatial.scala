package dpla.ingestion3.enrichments

// JavaConversions is for iterating over Sets of
// org.eclipse.rdf4j.model.Resource and org.eclipse.rdf4j.model.Model
import dpla.ingestion3.mappers.rdf.DefaultVocabularies
import dpla.ingestion3.mappers.rdf.RdfValueUtils
import org.apache.log4j.Logger
import org.eclipse.rdf4j.model._
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.collection.JavaConversions._
import scalaj.http._


/*
 * See https://gist.github.com/markbreedlove/b93e8bfca73b430e034af2e073c8112e
 * for a demo of how to get a field out of one of our item aggregations.
 * The code here departs from it and goes straight for the node that's an
 * edm:Place, but it's similar.
 */

/*
 * Example incoming mapped data from Ingestion 2, Digital Commonwealth.
 * Note big newline-delimited string with multiple place names. Note bad
 * geo:lat value that contains both lat and long -- not sure if that's something
 * that's been fixed. It's technically a data-cleanup issue.
 *
 * [ ... abridged ... ]
 * @prefix dc: <http://purl.org/dc/terms/> .
 * @prefix dpla: <http://dp.la/about/map/> .
 * @prefix edm: <http://www.europeana.eu/schemas/edm/> .
 * @prefix geo: <http://www.w3.org/2003/01/geo/wgs84_pos#> .
 * @prefix skos: <http://www.w3.org/2004/02/skos/core#> .
 * [...]
 * <http://ldp.staging.dp.la/ldp/items/00ea7274bf61874a7b8df90eb0042ef0> a <http://www.w3.org/ns/ldp#Resource>,
 * [...]
 *   dc:spatial [
 *    a edm:Place;
 *    dpla:providedLabel """
 *     North and Central America
 *     United States
 *     Texas
 *     Dallas
 *     Dallas
 *   """;
 *    geo:lat "32.7833,-96.80";
 *    skos:exactMatch "http://vocab.getty.edu/tgn/7013503"
 *  ];
 *
 * ... Otherwise, the one other provider whose unenriched RDF I've examined,
 * NARA, has one-line strings for the spatial statements:
 *
 *   dc:spatial [
 *    a edm:Place;
 *    dpla:providedLabel "Arlington (Va.)"
 *  ];
 *
 * Example output enriched data from Ingestion 2, Cal. Digital Library:
 *
 *  dc:spatial [
 *    a edm:Place;
 *    dpla:providedLabel "Los Angeles (Calif.)";
 *    gn:countryCode "US";
 *    geo:lat 3.405223e1;
 *    geo:long -1.1824368e2;
 *    skos:closeMatch <http://id.loc.gov/authorities/names/n79021240>;
 *    skos:exactMatch <http://sws.geonames.org/5368361/>;
 *    skos:prefLabel "Los Angeles, CA, United States"
 *  ];
 *
 * Note that Ingestion 2 (specifically, the Audumbla::CoarseGeocode enrichment)
 * was bailing on the assignment of parent features, probably due to the fact
 * that MAPv4 specifies them as URIs, which is inconvenient for us when it comes
 * time to serialize the records into JSON-LD and have it indexed effectively
 * by Elasticsearch, where you want strings for the parent features (E.g.
 * "Los Angeles County" or "California"), not URIs. Our users search for those
 * strings!
 *
 * We have work to do to determine if MAPv4 is actually the right thing, and
 * whether we need a revision; or if we want to jump through hoops to resolve
 * parent features as part of the process to writing JSON for Elasticsearch to
 * index.
 */

/* Case classes for JSON parser ... */
case class Coords(lat: Double, lng: Double)

case class Bounds(ne: Coords, sw: Coords)

case class Geometry(center: Coords, bounds: Bounds)

case class Feature(geometry: Geometry, woeType: Int, displayName: String)

case class Interpretation(feature: Feature)

case class Result(interpretations: List[Interpretation])

object Spatial extends RdfValueUtils with DefaultVocabularies {

  implicit val formats = DefaultFormats // Formats for json4s
  val log = Logger.getLogger(getClass.getName)

  def enrich(m: Model): Model = {

    val spatialModel = m.filter(null, rdf.`type`, edm.Place)
    if (spatialModel.isEmpty) return m

    for {
      s <- spatialModel.subjects
      provLabels = m.filter(s, dpla.providedLabel, null)
      pl <- provLabels
    } {

      /* We expect the provided label below not to need any special processing,
       * like stripping newlines, etc. That should be handled by an earlier
       * enrichment.
       *
       * If it's a multiline string, as we've encountered with Digital
       * Commonwealth, I really don't know if we can handle it well. Twofishes
       * will not do what you want with a string like "Dallas Dallas Texas
       * United States North and Central America". This particular instance will
       * work if the values are reversed (as "North and Central America United
       * States Texas Dallas Dallas") but I wouldn't want to count on that in
       * general.
       *
       * We need some research to determine how common this is. Ideally we'd
       * have multiple dc:spatial statements instead of one statement with a
       * multiline string as the object.
       *
       * Is this an issue with Krikri that won't happen with Ingestion 3?
       */

      try {

        val providedLabel = pl.getObject.stringValue
        m.add(s, dpla.providedLabel, literal(providedLabel))

        // Look up the place by name (providedLabel) ...
        val baseURI = "http://geo-prod:8081/query"
        val response: HttpResponse[String] =
          Http(baseURI).param("lang", "en")
            .param("responseIncludes", "PARENTS,DISPLAY_NAME")
            .param("query",  providedLabel).asString
        val jsonDoc = parse(response.body)

        val name = ((jsonDoc \ "interpretations") (0) \ "feature")
          .extract[Feature].displayName
        val coords = ((((jsonDoc \ "interpretations") (0) \ "feature") \ "geometry") \ "center")
          .extract[Coords]
        m.add(s, skos.prefLabel, literal(name))
        m.add(s, wgs84.lat, literal(coords.lat.toString))
        m.add(s, wgs84.long, literal(coords.lng.toString))

        // Next do something with parents (requires adding case classes above
        // for parent features, etc.)

      } catch {
        // Best way to log a message?
        case e: java.lang.IndexOutOfBoundsException => {
          log.error(s"No interpretations for ${pl.getObject.stringValue}")
        }
        case e: Exception => {
          log.error(e.toString)
        }
      }

    } // for

    m
  } // enrich
}
