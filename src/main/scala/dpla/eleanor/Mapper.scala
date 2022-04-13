package dpla.eleanor

import dpla.eleanor.Schemata.HarvestData
import dpla.ingestion3.executors.DplaMap
import dpla.ingestion3.model.OreAggregation
import org.apache.spark.sql.{Dataset, _}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._

object Mapper {

  def execute(spark: SparkSession, harvest: Dataset[HarvestData]): Dataset[OreAggregation] = {
    val window: WindowSpec = Window.partitionBy("id").orderBy(col("timestamp").desc)
    val dplaMap = new DplaMap()

    import spark.implicits._

    val mappedData = harvest
      .withColumn("rn", row_number.over(window))
      .where(col("rn") === 1).drop("rn") // get most recent versions of each record ID and drop all others
      .select("metadata", "sourceUri")
      // @see ProviderRegistry where I've double registered GPO under 'gpo' and 'http://gpo.gov' pointing to the same
      // ProviderProfile
      .map(harvestRecord => {
        val metadata = harvestRecord.get(0).asInstanceOf[Array[Byte]].map(_.toChar).mkString // convert Array[Byte] to String
        val shortName =  harvestRecord.getString(1) // Source.uri as providerProfile short name
        dplaMap.map(metadata, shortName)
      })

    mappedData
  }
}


