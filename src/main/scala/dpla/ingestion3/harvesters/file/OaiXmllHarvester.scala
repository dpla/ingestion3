package dpla.ingestion3.harvesters.file

import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.harvesters.Harvester
import dpla.ingestion3.mappers.utils.XmlExtractor
import dpla.ingestion3.model.AVRO_MIME_XML
import org.apache.avro.generic.GenericData
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.xml.{NodeSeq, XML}

class OaiXmllHarvester(
    spark: SparkSession,
    shortName: String,
    conf: i3Conf
) extends Harvester(spark, shortName, conf) {

  val logger = LogManager.getLogger(this.getClass)

  override def harvest: DataFrame = {
    val in = conf.harvest.endpoint.getOrElse("in")
    val inDf = spark.read.text(in)

    val extractId = (xmlString: String) =>
      (XML.loadString(xmlString) \ "header" \ "identifier").text
    val extractIdUdf = udf(extractId)

    val ingestDate = System.currentTimeMillis() / 1000L
    inDf
      .withColumn("id", extractIdUdf(col("value")))
      .withColumnRenamed("value", "document")
      .withColumn("ingestDate", lit(ingestDate))
      .withColumn("provider", lit(shortName))
      .withColumn("mimetype", lit(mimeType.toString))

  }

  override def mimeType: GenericData.EnumSymbol = AVRO_MIME_XML

  override def localHarvest(): DataFrame = ???
}