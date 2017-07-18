package dpla.ingestion3.harvesters.oai

import org.apache.spark.sql._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD

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
                   blacklist: Option[Array[String]])
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

  def allSets: RDD[OaiSet] = oaiResponseBuilder.getSets

  /**
    * Make appropriate call to OaiResponseBuilder based on presence or absence of
    * set, blacklist, or harvetAllSets.
    */
  def records: RDD[OaiRecord] = {

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

    val sets: Option[Array[OaiSet]] = {
      (setlist, blacklist, harvestAllSets) match {
        // If setlist is present, get all sets and subtract any that are not
        // included in the setlist.
        case(Some(whiteSets), None, false) => {
          Some(allSets.collect.filter(whiteSets contains _.id))
        }
        // If blacklist is present, get all sets and subtract any blacklisted sets.
        case (None, Some(blackSets), false) => {
          Some(allSets.collect.filterNot(blackSets contains _.id))
        }
        // If harvestAllSets is true, get all sets.
        case (None, None, true) => Some(allSets.collect)
        case (None, None, false) => None
        // Throw exception if more than one set handler is present.
        case _ => {
          val msg = "Only one of the following can be present: " +
            "harvestAllSets, setlist, blacklist"
          throw new IllegalArgumentException(msg)
        }
      }
    }

    sets match {
      case Some(s) => oaiResponseBuilder.getRecordsBySets(s, options)
      case _ => oaiResponseBuilder.getRecords(options)
    }
  }

  // Set the schema for the DataFrame that will be returned on load.
  override def schema: StructType = {

    verb match {
      case "ListSets" => {
        StructType(Seq(StructField("id", StringType, true),
          StructField("document", StringType, true)))
      }
      case "ListRecords" => {
        StructType(Seq(StructField("id", StringType, true),
          StructField("document", StringType, true),
          StructField("set_id", StringType, true),
          StructField("set_document", StringType, true)))
      }
    }
  }

  // Build the rows for the DataFrame.
  override def buildScan(): RDD[Row] = {

    verb match {
      case "ListSets" => {
        allSets.map(set => Row(set.id, set.document))
      }
      case "ListRecords" => {
        records.map(record => {
          // Get set id and set document if record has associated set.
          val (setId, setDoc) = record.set match {
            case Some(set) => (set.id, set.document)
            case _ => (None, None)
          }
          Row(record.id, record.document, setId, setDoc)
        })
      }
      case _ => {
        val msg = s"Verb $verb not supported."
        throw new IllegalArgumentException(msg)
      }
    }
  }
}
