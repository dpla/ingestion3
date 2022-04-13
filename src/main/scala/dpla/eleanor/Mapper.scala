package dpla.eleanor

import dpla.eleanor.Schemata.Implicits._
import dpla.eleanor.Schemata.SourceUri._
import dpla.eleanor.Schemata.{HarvestData, MappedData}
import dpla.eleanor.mappers._
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Encoders}

object Mapper {

  def execute(harvest: Dataset[HarvestData]): Dataset[MappedData] = {
    val window: WindowSpec = Window.partitionBy("id").orderBy(col("timestamp").desc)

    harvest
      .withColumn("rn", row_number.over(window))
      .where(col("rn") === 1).drop("rn") // get most recent versions of each record ID and drop all others
      .as(Encoders.product[HarvestData])
      .flatMap(x => map(x))
  }

  def map(input: HarvestData): Option[MappedData] =
    input.sourceUri match {
      case StandardEbooks.uri => StandardEbooksMapping.map(input)
      case Feedbooks.uri => FeedbooksMapping.map(input)
      case UnglueIt.uri => UnglueItMapping.map(input)
      case Gutenberg.uri => GutenbergMapping.map(input)
      //case Gpo.uri => GpoMapping.map(input)
      case _ => None
    }
}


