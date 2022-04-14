package dpla.eleanor

import dpla.eleanor.Schemata.{Ebook, HarvestData}
import dpla.eleanor.profiles.{EbookProfile, EbookProviderRegistry}
import dpla.ingestion3.executors.EbookMap
import dpla.ingestion3.model.OreAggregation
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, _}
import org.json4s.JsonAST.JValue

import scala.util.{Failure, Success}
import scala.xml.NodeSeq

object Mapper {

  def execute(spark: SparkSession, harvest: Dataset[HarvestData]): Dataset[Ebook] = {
    val window: WindowSpec = Window.partitionBy("id").orderBy(col("timestamp").desc)
    val dplaMap = new EbookMap()

    import spark.implicits._

    harvest
      .withColumn("rn", row_number.over(window))
      .where(col("rn") === 1).drop("rn") // get most recent versions of each record ID and drop all others
      .select("metadata", "sourceUri")
      .map(harvestRecord => {
        val metadata = harvestRecord.get(0).asInstanceOf[Array[Byte]].map(_.toChar).mkString // convert Array[Byte] to String
        val shortName =  harvestRecord.getString(1) // Source.uri as providerProfile short name
        dplaMap.map(metadata, getExtractorClass(shortName))
      })
  }

  /**
    * Look up provider profile in Ebooks Provider Registry
    * @param shortName
    * @return
    */
  def getExtractorClass(shortName: String): EbookProfile[_ >: NodeSeq with JValue] =
    EbookProviderRegistry.lookupProfile(shortName) match {
      case Success(extClass) => extClass
      case Failure(e) => throw new RuntimeException(s"Unable to load $shortName mapping from CHProviderRegistry")
    }
}


