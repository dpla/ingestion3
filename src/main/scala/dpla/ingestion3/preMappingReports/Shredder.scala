package dpla.ingestion3.premappingreports

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

trait Shredder extends Serializable {
  // Abstract methods to be over-written
  protected def getTriples(id: String, record: String): List[Triple]

  // Main entry point
  def process(harvestedData: DataFrame): DataFrame = {
    val records: DataFrame = harvestedData.select("document", "id")
      .where("document is not null")
      .distinct

    // Each row in `triples' contains a List of Triples.
    val triples = records
      .withColumn("triples", docToTriplesFunc(records.col("id"), records.col("document")))
      .drop("document", "id")

    // Each row in `flattened' contains a Triple.
    val flattened = triples.withColumn("triples", explode(triples.col("triples")))
      .where("triples is not null")

    // The returned DataFrame has three columns: "id", "attribute", "value"
    flattened.select("triples.id", "triples.attribute", "triples.value")
  }

  /**
    * Create a UDF to convert a document String into a List of parsed Triples.
    */
  private val docToTriples: ((String, String) => List[Triple]) =
    (id: String, record: String) => getTriples(id, record)
  private val docToTriplesFunc = udf(docToTriples)
}
