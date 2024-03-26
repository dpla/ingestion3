package dpla.ingestion3.entries.wiki

import dpla.ingestion3.confs.CmdArgs
import dpla.ingestion3.executors.WikimediaMetadataExecutor
import org.apache.spark.SparkConf

object WikiMetadataEntry extends WikimediaMetadataExecutor {

  /** Driver for reading DplaMapData records (mapped or enriched) and generating
    * Wikidata markup as a part of a JSON record.
    *
    * The metadata/wikimedia markup can be ingested to Wikimedia Commons The
    *
    * Expects three parameters: 1) a path to the enriched data 2) a path to
    * output the JSON data 3) provider short name (e.g. 'mdl', 'cdl', 'harvard')
    * 4) spark master (optional parameter that overrides a --master param
    * submitted via spark-submit
    *
    * Usage ----- To invoke via sbt: sbt "run-main
    * dpla.ingestion3.WikiMetadataEntry --input=/input/path/to/enriched/
    * --output=/output/path/to/wikimedia/ --name=shortName"
    * --sparkMaster=local[*]
    */
  def main(args: Array[String]): Unit = {

    // Read in command line args.
    val cmdArgs = new CmdArgs(args)

    val dataIn = cmdArgs.getInput
    val dataOut = cmdArgs.getOutput
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
