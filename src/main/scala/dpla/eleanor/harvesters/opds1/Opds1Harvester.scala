package dpla.eleanor.harvesters.opds1

import java.sql.Timestamp

import dpla.eleanor.HarvestStatics
import dpla.eleanor.Schemata.{HarvestData, MetadataType, SourceUri}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.{Failure, Success}

class Opds1Harvester(timestamp: Timestamp,
                     source: SourceUri,
                     metadataType: MetadataType)
  extends Opds1FeedHarvester with Opds1FileHarvester with Serializable {
  /**
    *
    * @param spark            Spark session
    * @param feedUrl          Feed URL to harvest
    * @return
    */
  def execute(spark: SparkSession,
              feedUrl: Option[String],
              xmlFiles: Seq[String] = Seq()): Dataset[HarvestData] = {

    import spark.implicits._

    val harvestStatics: HarvestStatics = HarvestStatics(
      sourceUri = source.uri,
      timestamp = timestamp,
      metadataType = metadataType
    )

    // If feed url is defined, then harvests that feed to a file
    // If feed url is `not` defined, then harvest records from specified files local files
    val files = feedUrl match {
      case Some(feed) =>
        harvestFeed(feed) match {
          case Success(file) => Seq(file)
          case Failure(f) => throw new Exception(s"Unable to harvest feed to file. Aborting harvest run. ${f.getMessage}")
        }
      case None => xmlFiles
    }
    // Harvest file into HarvestData
    val rows: Seq[HarvestData] = files.flatMap(xmlFile => harvestFile(xmlFile, harvestStatics))
    spark.createDataset(rows)
  }
}
