package dpla.ingestion3.wiki

import java.net.URL

import dpla.ingestion3.utils.HttpUtils
import org.json4s.JValue
import org.json4s.jackson.JsonMethods.parse

/** Fetches and caches the institutions_v2.json snapshot for the lifetime of
  * the JVM. Both [[WikiEntityEnrichment]] and [[WikiMapper]] load institution
  * data from the same file; fetching once ensures they operate on the same
  * snapshot even if the file changes mid-job.
  *
  * Fails loudly on HTTP error so that enrichment jobs do not silently proceed
  * with an empty vocabulary.
  */
object InstitutionsLoader {

  val INSTITUTIONS_URL: String =
    "https://raw.githubusercontent.com/dpla/ingestion3/refs/heads/main/src/main/resources/wiki/institutions_v2.json"

  lazy val institutions: JValue =
    parse(HttpUtils.makeGetRequest(new URL(INSTITUTIONS_URL)))
}
