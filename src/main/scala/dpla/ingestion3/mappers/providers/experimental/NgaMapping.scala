/**
 * TEST HUB — NOT APPROVED FOR PRODUCTION
 *
 * STUB MAPPER — placeholder only.
 *
 * This is intentionally minimal: it implements just the required `Mapping`
 * members so that `NgaProfile` compiles and the pipeline can dispatch an NGA
 * *harvest* (which does not invoke mapping). It does NOT map the NGA record and
 * must be replaced by the real `NgaMapping` before any mapping/enrichment run.
 *
 * Do not remove the `status = test` flag from i3.conf until the hub has been
 * formally approved. See docs/ingestion/README_TEST_HUBS.md.
 */
package dpla.ingestion3.mappers.providers.experimental

import dpla.ingestion3.mappers.utils.{Document, JsonExtractor, JsonMapping}
import dpla.ingestion3.model.DplaMapData.ZeroToOne
import org.json4s.JValue

class NgaMapping extends JsonMapping with JsonExtractor {

  override def useProviderName: Boolean = true

  override def getProviderName: Option[String] = Some("nga")

  // The harvester keys each assembled document by the NGA TMS objectid.
  override def originalId(implicit data: Document[JValue]): ZeroToOne[String] =
    extractString(unwrap(data) \ "objectid")
}
