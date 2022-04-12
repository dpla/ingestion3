package dpla.ingestion3.preMappingReports

import scala.annotation.tailrec
import org.json4s.JValue
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonAST._

/**
  * JSON shredder for pre-mapping QA.
  */
object JsonShredder extends Shredder {

  /**
    * Entry point for JsonShredder.
    * Shred a single record into Triples.
    *
    * A Triple is a id-attribute-value grouping.
    * It is terminal, meaning that the value is a String (rather than a JSON
    * key-value pair).
    *
    * Integers, booleans, and strings make triples.
    *
    * Any JSON parsing errors fail silently.
    *
    * @param id String ID of the record being shredded
    * @param record String the record being shredded
    * @return List[Triple]
    */
  def getTriples(id: String, record: String): List[Triple] = {
    val json: JValue = parse(record)
    shredRecord(id, json)
  }

  /**
    * Parse Triples from a root node of an JSON record.
    *
    * Child nodes are mapped to a case class called JValueWithLabel.
    * The label is actually the concatenated labels of a node and all its ancestors.
    * For example, given this json:
    *   { "a": { "b": { "c": "foo" } } }
    *   The label for the c node is "a_b_c"
    * The case class stores the label so that it doesn't have to be constructed
    * for each triple via another traversal of the JSON record.
    *
    * This method uses tail recursion to traverse the JSON document.
    * Starting with the root node, all strings, booleans, and numbers are
    * mapped to Triples and put in one "bucket."
    * All JObjects and JArray entries are put into another "bucket".
    *
    * Both "buckets" are passed back through the loop.  All nodes in the
    * bucket are parsed.  Any new triples are added to the  triples bucket,
    * and any new nodes are put into the nodes bucket.
    * This repeats until the nodes bucket is empty.
    *
    * @param id String The id of the record being shredded.
    * @param root The root XML node for a single record.
    * @return List[Triples] All of the parsed triples from the record.
    */
  private def shredRecord(id: String, root: JValue): List[Triple] = {

    /**
      * @param triples List[Triples] Accumulates all triples as xml doc is
      *                traversed.
      * @param nodes   List[JValueWithLabel] Contains all nodes to be parsed in a
      *                single loop cycle.
      * @return        List[Triple] All accumulated triples.
      */
    @tailrec
    def loop(triples: List[Triple], nodes: List[JValueWithLabel]): List[Triple] = {
      // Terminate loop if there are no more XML nodes to parse.
      if (nodes.isEmpty) triples
      else {
        // Otherwise, continue parsing nodes.

        // Parse all current nodes into new triples and new nodes.
        val shreds: List[(List[Triple], List[JValueWithLabel])] =
          nodes.map(n => shredNode(id, n))

        val newTriples: List[Triple] = shreds.flatMap{ case (triple, _) => triple }
        val newNodes: List[JValueWithLabel] = shreds.flatMap{ case (_, node) => node }

        loop(triples ::: newTriples, newNodes)
      }
    }

    val firstNode = JValueWithLabel("", root)
    loop(List(), List(firstNode))
  }

  /**
    * Shred a single JSON node.
    *
    * Numbers and boolean values are forced into String types.
    *
    * @param id String The id of the record being shredded.
    * @param node JValueWithLabel The XML node to be shredded.
    * @return (List[Triple], List[NodeWithLabels]  The complete triples and XML
    *         child nodes that are derived from the given node.
    */
  private def shredNode(id: String, node: JValueWithLabel): (List[Triple], List[JValueWithLabel]) = {
    val shreds: Seq[Shred] = node.jValue match {
      case JObject(fields) => parseJFields(fields, node.label)
      case JArray(array) => parseJArray(array, node.label)
      case _ => extractString(node.jValue) match {
        case Some(stringValue) => Seq(Triple(id, node.label, stringValue))
        case None => Seq()
      }
    }

    val jVals = shreds.filter(_.isInstanceOf[JValueWithLabel])
      .asInstanceOf[List[JValueWithLabel]]

    val triples = shreds.filter(_.isInstanceOf[Triple])
      .asInstanceOf[List[Triple]]

    (triples, jVals)
  }

  private def parseJArray(array: List[JValue], nodeLabel: String): Seq[JValueWithLabel] =
    array.map(jValue => JValueWithLabel(nodeLabel, jValue))

  private def parseJFields(fields: List[JField], nodeLabel: String): Seq[JValueWithLabel] =
    fields.map { case (label, value) =>
      val newLabel = concatLabel(nodeLabel, label)
      JValueWithLabel(newLabel, value)
    }

  private def extractString(jValue: JValue): Option[String] = jValue match {
    case JBool(bool) => Some(bool.toString)
    case JDecimal(decimal) => Some(decimal.toString())
    case JDouble(double) => Some(double.toString())
    case JInt(int) => Some(int.toString())
    case JString(string) => Some(string)
    case _ => None
  }

  /**
    * Join two label strings with "_"
    * Replace ":" with "." b/c ":" is a protected char in elastic search.
    */
  private def concatLabel(oldLabel: String, newLabel: String): String =
    Seq(oldLabel, newLabel).filter(_.nonEmpty).mkString("_").replaceAll(":", ".")
}
