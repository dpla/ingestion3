package dpla.ingestion3.premappingreports

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.util.parsing.json._

class XmlShredderReport(val inputURI: String,
                        val outputURI: String,
                        val sparkMasterName: String
                       ) extends PreMappingReport with Serializable {

  override val sparkAppName: String = "XmlShredder"

  override def getInputURI: String = inputURI

  override def getOutputURI: String = outputURI

  override def getSparkMasterName: String = sparkMasterName

  /**
    * Process the incoming DataFrame.
    *
    * The resulting RDD contains JSONType object.
    * Each JSONType object represents a single record, in a format that will
    * work with elastidump.
    *
    * @see PreMappingReport.process()
    * @param df DataFrame (harvested records)
    * @return RDD[JSONType]
    */
  override def process(df: DataFrame): RDD[JSONType] = {

    val records: DataFrame = df.select("document", "id")
      .where("document is not null")
      .distinct

    // Each row in `triples' contains a List of Triples.
    val triples = records
      .withColumn("triples", docToTriplesFunc(records.col("id"), records.col("document")))
      .drop("document", "id")

    // Each row in `flattened' contains a Triple.
    val flattened = triples.withColumn("triples", explode(triples.col("triples")))
      .where("triples is not null")

    // Each row in `flattenedAgain' has three columns: "id", "attribute", "value"
    val flattenedAgain = flattened
      .select("triples.id", "triples.attribute", "triples.value")

    // Each row in the returned DataFrame has columns for "id" and for each
    // attribute.  If there is more than one value for any attribute, it is
    // concatenated with ",".
    val grouped = flattenedAgain.groupBy("id")
      .pivot("attribute")
      .agg(concat_ws(",", collect_list(col("value"))))

    // Change each row to a JSON String.
    val json: Dataset[String] = grouped.toJSON

    // Covert Dataset to RDD.  Datasets run into encoder errors when you try to
    // parse their Rows to JSON.
    json.rdd.flatMap(x => {
      // Create proper nested JSON format to work with elasticdump.
      val string = """{"_source": """ + x + """}"""
      // Convert Strings into JSONType objects so they can be written out to
      // JSON files.
      JSON.parseRaw(string)
    })
  }

  /**
    * Create a UDF to convert a document String into a List of parsed Triples.
    */
  private val docToTriples: ((String, String) => List[Triple]) =
    (id: String, record: String) => XmlShredder.getXmlTriples(id, record)

  private val docToTriplesFunc = udf(docToTriples)
}
