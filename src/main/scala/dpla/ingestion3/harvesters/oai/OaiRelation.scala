package dpla.ingestion3.harvesters.oai

import org.apache.spark.sql._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.ScalaReflection

/**
  * This class interprets the user-submitted params to determine which type of
  * request to send to the OAI feed (ie. get records or sets of records).
  * It requests an OAI response via the `OaiResponseBuilder`.
  * It constructs a DataFrame from the OAI response.
  */
class OaiRelation (endpoint: String,
                   verb: String,
                   metadataPrefix: Option[String],
                   harvestAllSets: Boolean,
                   setlist: Option[Array[String]],
                   blacklist: Option[Array[String]],
                   removeDeleted: Boolean
                  )
                  (@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan with Serializable {

  // Get all possible instructions on how to handle sets.
  def setHandler: List[Any] = {
    val setHandlers = List(harvestAllSets, setlist, blacklist)
    setHandlers.filterNot(x => (x == None || x == false))
  }

  // Ensure that set handlers are only used with the "ListRecords" verb.
  require((setHandler.size == 0 || verb == "ListRecords"), "The following can " +
    "only be used with the 'ListRecords' verb: harvestAllSets, setlist, blacklist")

  val oaiResponseBuilder = new OaiResponseBuilder(endpoint)(sqlContext)

  def allSets: RDD[OaiResponse] = oaiResponseBuilder.getAllSets

  /**
    * Make appropriate call to OaiResponseBuilder based on presence or absence of
    * set, blacklist, or harvetAllSets.
    */
  def records: RDD[OaiResponse] = {

    /**
      * Get any optional OAI arguments that may be used in a records request.
      * Remove any null values.
      * Currently, the only supported option for a records request is metadataPrefix.
      * Per the OAI spec, the only required OAI args are "verb" and "endpoint";
      * every OAI request must include these two args.
      * All other args are permissible only in certain contexts.
      * The OaiResponseBuilder is responsible for managing these contexts.
      */
    def options: Map[String, String] = {
      Map("metadataPrefix" -> metadataPrefix).collect {
        case(key, Some(value)) => key -> value
      }
    }

    setResponses match {
      case Some(sets) =>
        // If there are sets to be harvested from:
        oaiResponseBuilder.getRecordsBySets(sets, options, removeDeleted)
      case None =>
        // If there are no sets to be harvested from:
        oaiResponseBuilder.getAllRecords(options, removeDeleted)
    }
  }

  /**
    * Get any relevant sets for a records request, based on presence of setlit,
    * blacklist, or harvestAllSets.
    *
    * @return all sets from which records should be harvested, and any errors
    *         incurred in the process of harvesting said sets
    */
  def setResponses: Option[RDD[OaiResponse]] = {
    (setlist, blacklist, harvestAllSets) match {
      case (Some(list), None, false) =>
        Some(oaiResponseBuilder.getSetsInclude(list))
      case (None, Some(list), false) =>
        Some(oaiResponseBuilder.getSetsExclude(list))
      case (None, None, true) => Some(allSets)
      case (None, None, false) => None
      // Throw exception if more than one set handler is present.
      case _ =>
        val msg = "Only one of the following can be present: " +
          "harvestAllSets, setlist, blacklist"
        throw new IllegalArgumentException(msg)
    }
  }

  // Get responses according to the verb.
  def getResponses: RDD[OaiResponse] = {
    verb match {
      case "ListSets" => allSets
      case "ListRecords" => records
      case _ =>
        val msg = s"Verb $verb not supported."
        throw new IllegalArgumentException(msg)
    }
  }

  /**
    * Set the schema for the DataFrame that will be returned on load.
    * Columns:
    *   - set: OaiSet
    *   - record: OaiRecord
    *   - error: String
    * Each row will have a non-null value for ONE of sets, records, or error.
    */
  override def schema: StructType = {
    val setType = ScalaReflection.schemaFor[OaiSet].dataType
      .asInstanceOf[StructType]
    val recordType = ScalaReflection.schemaFor[OaiRecord].dataType
      .asInstanceOf[StructType]
    val errorType = ScalaReflection.schemaFor[OaiError].dataType
      .asInstanceOf[StructType]

    val setField = StructField("set", setType, true)
    val recordField = StructField("record", recordType, true)
    val errorField = StructField("error", errorType, true)
    StructType(Seq(setField, recordField, errorField))
  }

  /**
    * Build the rows for the DataFrame.
    *
    * Duplicate records may occur if they appeared in different sources.
    * For example, if a record belongs to more than one set, it may appear in
    * responses for each set.
    */
  override def buildScan(): RDD[Row] = {

    getResponses.flatMap {
      case x: SetsPage => x.sets.map(set => Row(set, None, None))
      case x: RecordsPage => x.records.map(record => Row(None, record, None))
      case x: OaiError => Seq(Row(None, None, x))
      case x: OaiSource =>
        val msg = "Unexpected OaiSource type encountered when building DataFrame"
        val exception = new OaiHarvesterException(msg)
        val error = OaiError(exception.toString, x)
        Seq(Row(None, None, error))
    }
  }
}
