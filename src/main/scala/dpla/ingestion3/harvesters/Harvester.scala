package dpla.ingestion3.harvesters

import dpla.ingestion3.utils.FlatFileIO
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

import scala.xml._

abstract class Harvester {

  def harvest: DataFrame

  def mimeType: GenericData.EnumSymbol

  def close(): Unit = ()

  def cleanUp(): Unit = ()

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
