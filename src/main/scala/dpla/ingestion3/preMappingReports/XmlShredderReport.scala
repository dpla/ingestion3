package dpla.ingestion3.premappingreports

import org.apache.spark.sql._
import org.apache.spark.sql.functions.{udf, explode}

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
    * The resulting DataFrame has the following columns:
    *   - id: String
    *   - attribute: String
    *   - value: String
    *
    * @see PreMappingReport.process()
    * @param df DataFrame (harvested records)
    * @return DataFrame
    */
  override def process(df: DataFrame): DataFrame = {

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

    flattened.select("triples.id", "triples.attribute", "triples.value")
  }

  /**
    * Create a UDF to convert a document String into a List of parsed Triples.
    */
  private val docToTriples: ((String, String) => List[Triple]) =
    (id: String, record: String) => XmlShredder.getXmlTriples(id, record)

  private val docToTriplesFunc = udf(docToTriples)
}
