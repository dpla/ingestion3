package dpla.eleanor.harvesters.opds1

import java.sql.Timestamp

import dpla.eleanor.HarvestStatics
import dpla.eleanor.Schemata.{HarvestData, MetadataType, SourceUri}
import dpla.eleanor.harvesters.ContentHarvester
import dpla.ingestion3.dataStorage.OutputHelper
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import scala.util.{Failure, Success}

class Opds1Harvester(timestamp: Timestamp,
                     source: SourceUri,
                     sourceName: String,
                     metadataType: MetadataType)
  extends Opds1FeedHarvester with Opds1FileHarvester with ContentHarvester with Serializable {
  /**
    *
    * @param spark            Spark session
    * @param feedUrl          Feed URL to harvest
    * @param rootOutput       Could be local or s3
    * @return
    */
  def execute(spark: SparkSession,
              feedUrl: Option[String],
              xmlFiles: Seq[String] = Seq(),
              rootOutput: String): Dataset[HarvestData] = {

    import spark.implicits._

    val harvestStatics: HarvestStatics = HarvestStatics(
      sourceUri = source.uri,
      timestamp = timestamp,
      metadataType = metadataType
    )

    // Harvests feed to file
    // If feed url is given then harvests feed to file, if no feed url is given then try to harvest from specified files
    val files = feedUrl match {
      case Some(feed) => {
        harvestFeed(feed) match {
          case Success(file) => Seq(file)
          case Failure(f) => throw new Exception(s"Unable to harvest feed to file. Aborting harvest run. ${f.getMessage}")
        }
      } case None => xmlFiles
    }

    println("Harvested feed to file")

    // Harvest file into HarvestData
    val rows: Seq[HarvestData] = files.flatMap(xmlFile => harvestFile(xmlFile, harvestStatics))
    val ds = spark.createDataset(rows)

    println(s"Harvested ${ds.count()} from ${files.foreach(_.toString + "\n")} files")

    // Run over HarvestData Dataset and download content/payloads
    val contentDs = ds // harvestContent(ds, spark)

    val outputHelper: OutputHelper =
      new OutputHelper(rootOutput, sourceName, "ebook-harvest", timestamp.toLocalDateTime)

    val outputPath = outputHelper.activityPath
    println(s"Writing to $outputPath")
    contentDs.write.mode(SaveMode.Append).parquet(outputPath)

    contentDs
  }
}
