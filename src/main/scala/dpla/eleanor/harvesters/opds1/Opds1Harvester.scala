package dpla.eleanor.harvesters.opds1

import java.sql.Timestamp

import dpla.eleanor.HarvestStatics
import dpla.eleanor.Schemata.{HarvestData, MetadataType, SourceUri}
import dpla.eleanor.harvesters.ContentHarvester
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import scala.util.{Failure, Success}

class Opds1Harvester(timestamp: Timestamp, source: SourceUri, metadataType: MetadataType)
  extends Opds1FeedHarvester with Opds1FileHarvester with ContentHarvester with Serializable {
  /**
    *
    * @param spark            Spark session
    * @param feedUrl          Feed URL to harvest
    * @param out              Could he local or s3
    * @return
    */
  def execute(spark: SparkSession,
              feedUrl: Option[String],
              xmlFiles: Seq[String] = Seq(),
              out: String): Dataset[HarvestData] = {

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
    val contentDs = harvestContent(ds, spark)

    // Write out complete HarvestData Dataset
    val dataOut = out + "data.parquet/" // fixme hardcoded output
    println(s"Writing to $dataOut")
    contentDs.write.mode(SaveMode.Append).parquet(dataOut)

    contentDs
  }
}
