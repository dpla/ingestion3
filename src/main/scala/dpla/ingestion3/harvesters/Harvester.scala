package dpla.ingestion3.harvesters

import java.io.File
import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.harvesters.file.ParsedResult
import dpla.ingestion3.utils.{FlatFileIO, Utils}
import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.commons.io.FileUtils
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.xml._

abstract class Harvester(
    spark: SparkSession,
    shortName: String,
    conf: i3Conf
) {

  def harvest: DataFrame

  def mimeType: GenericData.EnumSymbol

  def cleanUp(): Unit = ()

}

/** Abstract class for local harvesters.
  *
  * The harvester abstract class has methods to manage aspects of a harvest that
  * are common among all providers, including:
  *   1. Provide an entry point to run a harvest.
  *   2. If the output directory  already exists, delete its contents.
  *   3. Log and timestamp the beginning and end of a harvest.
  *   4. Log information about the completed harvest.
  *   5. Validate the schema final DataFrame (only for logging purposes). 6.
  *      Manage the spark session.
  *
  * @param shortName
  *   [String] Provider short name
  * @param conf
  *   [i3Conf] contains configs for the harvester.
  */
abstract class LocalHarvester(
    spark: SparkSession,
    shortName: String,
    conf: i3Conf,
) extends Harvester(spark, shortName, conf) {


  override def harvest: DataFrame = {
    localHarvest()
  }

  def localHarvest(): DataFrame


  // Temporary output path.
  // Harvests that use AvroWriter cannot be written directly to S3.
  // Instead, they are written to this temp path,
  //   then loaded into a spark DataFrame,
  //   then written to their final destination.
  // TODO: make tmp path configurable rather than hard-coded
  val tmpOutStr: String =
    new File(FileUtils.getTempDirectory, shortName).getAbsolutePath

  // Delete temporary output directory and files if they already exist.
  Utils.deleteRecursively(new File(tmpOutStr))

  private val avroWriter: DataFileWriter[GenericRecord] =
    AvroHelper.avroWriter(shortName, tmpOutStr, Harvester.schema)

  def getAvroWriter: DataFileWriter[GenericRecord] = avroWriter

  def writeOut(unixEpoch: Long, item: ParsedResult): Unit = {
    val avroWriter = getAvroWriter
    val genericRecord = new GenericData.Record(Harvester.schema)
    genericRecord.put("id", item.id)
    genericRecord.put("ingestDate", unixEpoch)
    genericRecord.put("provider", shortName)
    genericRecord.put("document", item.item)
    genericRecord.put("mimetype", mimeType)
    avroWriter.append(genericRecord)
  }

  override def cleanUp(): Unit = {
    avroWriter.flush()
    avroWriter.close()
    // Delete temporary output directory and files.
    Utils.deleteRecursively(new File(tmpOutStr))
  }
}

object Harvester {


  /** Converts a Node to an xml string
   *
   * @param node
   *   The root of the tree to write to a string
   * @return
   *   a String containing xml
   */
  def xmlToString(node: Node): String =
    Utility.serialize(node, minimizeTags = MinimizeMode.Always).toString


  // Schema for harvested records.
  val schema: Schema =
    new Schema.Parser()
      .parse(new FlatFileIO().readFileAsString("/avro/OriginalRecord.avsc"))

  def validateSchema(df: DataFrame): Unit = {
    val idSt = StructField("id", StringType, nullable = true)
    val docSt = StructField("document", StringType, nullable = true)
    val dateSt = StructField("ingestDate", LongType, nullable = false)
    val provSt = StructField("provider", StringType, nullable = false)
    val mimeSt = StructField("mimetype", StringType, nullable = false)

    // Match the fields within the schema, rather than the schema itself.
    // This allows DataFrames where the fields are in different orders to pass
    // the logical test.
    val expectedStructs = Array(idSt, docSt, dateSt, provSt, mimeSt)
    val actualStructs = df.schema.fields

    // Match only the names and data types of the fields.
    // Whether or not a field is nullable does not matter for our purposes.
    def mapFields(fields: Array[StructField]): Array[(String, DataType)] =
      fields.map { s => s.name -> s.dataType }

    val expectedFields = mapFields(expectedStructs)
    val actualFields = mapFields(actualStructs)

    if (actualFields.diff(expectedFields).length > 0) {
      val msg =
        s"""Harvested DataFrame did not match expected schema.\n
        Actual fields: ${actualFields.mkString(", ")}\n
        Expected fields: ${expectedFields.mkString(", ")}"""
      LogManager.getLogger(this.getClass).warn(msg)
    }
  }
}
