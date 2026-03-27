package dpla.ingestion3.entries.wiki

import dpla.ingestion3.confs.CmdArgs
import dpla.ingestion3.executors.WikimediaMetadataExecutor
import org.apache.spark.SparkConf

object WikiMetadataEntry extends WikimediaMetadataExecutor {

  /** Driver for reading enriched AVRO records and generating a wiki-eligible
    * parquet for Wikimedia Commons ingestion.
    *
    * Usage (sbt):
    * {{{
    *   sbt "run-main dpla.ingestion3.entries.wiki.WikiMetadataEntry
    *         --input=s3://dpla-master-dataset/pa/enrichment/<datestamp>-pa-MAP4_0.EnrichRecord.avro/
    *         --output=s3://dpla-master-dataset/pa/
    *         --name=pa
    *         --sparkMaster=local[*]"
    * }}}
    */
  def main(args: Array[String]): Unit = {

    val cmdArgs = new CmdArgs(args)

    val dataIn    = cmdArgs.getInput
    val dataOut   = cmdArgs.getOutput
    val shortName = cmdArgs.getProviderName
    val sparkMaster: Option[String] = cmdArgs.getSparkMaster

    val baseConf =
      new SparkConf()
        .setAppName(s"Wiki Metadata Export: $shortName")

    val sparkConf = sparkMaster match {
      case Some(m) => baseConf.setMaster(m)
      case None    => baseConf
    }

    executeWikimediaMetadata(sparkConf, dataIn, dataOut, shortName)
  }
}
