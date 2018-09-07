package dpla.ingestion3.utils

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object OutputHelper {

  /*
   * Get output path for a harvest, mapping, enrichment, or indexing activity.
   *
   * @example:
   *   outputPath("s3://dpla-master-dataset", "cdl", "harvest") =>
   *   "s3://dpla-master-dataset/cdl/harvest/20170209_104428-cdl-OriginalRecord.avro"
   *
   * @see https://digitalpubliclibraryofamerica.atlassian.net/wiki/spaces/TECH/pages/84512319/Ingestion+3+Storage+Specification
   */
  def outputPath(directory: String, shortName: String, activity: String): String = {

    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")
    val timestamp: String = LocalDateTime.now.format(formatter)

    // TODO: handle "reports" case
    // TODO: handle "logs" case
    val schema = activity match {
      case "harvest" => "OriginalRecord"
      case "mapping" => "MAP4_0.MAPRecord"
      case "enrichment" => "MAP4_0.EnrichRecord"
      case "jsonl" => "MAP3_1.IndexRecord"
      case _ => throw new RuntimeException(s"Activity '$activity' not recognized")
    }

    val fileType = if (activity == "jsonl") "jsonl" else "avro"

    val dirWithSlash = if (directory.endsWith("/")) directory else s"$directory/"

    s"$dirWithSlash$shortName/$activity/$timestamp-$shortName-$schema.$fileType"
  }
}
