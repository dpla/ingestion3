package dpla.ingestion3.premappingreports

import scala.annotation.tailrec
import scala.util.{Try, Failure, Success}
import scala.xml._

/**
  * XML shredder for pre-mapping QA.
  */
object XmlShredder {

  /**
    * Entry point for XmlShredder.
    * Shred a single record into Triples.
    *
    * A Triple is a id-attribute-value grouping.
    * It is terminal, meaning that the value is a String (rather than another
    * XML node).
    * Attributes, xml namespaces, and text make triples.
    * Examples:
    *   <a id='foo'/>
    *   id: [ID of record being shredded]
    *   attribute: a.@id
    *   value:     foo
    *
    *   <a>foo</a>
    *   id: [ID of record being shredded]
    *   attribute: a.text()
    *   value:     foo
    *
    *   <a><b><c>foo</c></b></a>
    *   id: [ID of record being shredded]
    *   attribute: a.b.c.text()
    *   value:     foo
    *
    * Any XML parsing errors fail silently. This is based on these assumptions:
    *   1. Parse errors are unlikely since invalid XML causes failure during harvest.
    *   2. It is not the job of this pre-mapping report to identify invalid XML.
    *
    * @param id String ID of the record being shredded
    * @param record String the record being shredded
    * @return List[Triple]
    */
  def getXmlTriples(id: String, record: String): List[Triple] = {
    loadXml(record) match {
      case Success(xml) => shredRecord(id, xml)
      // Return empty list in case of XML parsing error.
      case Failure(_) => List()
    }
  }

  // Try to parse a string into valid XML.
  private def loadXml(string: String): Try[Node] = Try { XML.loadString(string) }

  /**
    * Parse Triples from a root node of an XML record.
    *
    * Child nodes are mapped to a case class called NodeWithLabel.
    * The label is actually the concatenated labels of a node and all its ancestors.
    * For example, given this xml:
    *   <a><b><c>foo</c></b></a>
    *   The label for the c node is "a.b.c"
    * The case class stores the label so that it doesn't have to be constructed
    * for each triple via another traversal of the XML record.
    *
    * This method uses tail recursion to traverse the XML document.
    * Starting with the root node, all attributes, xmlns, and text of the root are
    * mapped to Triples and put in one "bucket."  All child nodes are put into
    * another "bucket".
    *
    * Example:
    *   <root id='foo'><a>bar</a></root>
    *   Parsing the root would yield one triple and one node.
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
  private def shredRecord(id: String, root: Node): List[Triple] = {

    /**
      * @param triples List[Triples] Accumulates all triples as xml doc is
      *                traversed.
      * @param nodes   List[NodeWithLabel] Contains all nodes to be parsed in a
      *                single loop cycle.
      * @return        List[Triple] All accumulated triples.
      */
    @tailrec
    def loop(triples: List[Triple], nodes: List[NodeWithLabel]): List[Triple] = {
      // Terminate loop if there are no more XML nodes to parse.
      if (nodes.isEmpty) triples
      else {
        // Otherwise, continue parsing nodes.

        // Parse all current nodes into new triples and new nodes.
        val shreds: List[(List[Triple], List[NodeWithLabel])] =
          nodes.map(n => shredNode(id, n))

        val newTriples: List[Triple] = shreds.flatMap{ case (triple, _) => triple }
        val newNodes: List[NodeWithLabel] = shreds.flatMap{ case (_, node) => node }

        loop(triples ::: newTriples, newNodes)
      }
    }

    val firstFragment = NodeWithLabel(labelWithPrefix(root), root)
    loop(List(), List(firstFragment))
  }

  /**
    * Shred a single XML node.
    *
    * Attribute and text values are forced into String types.
    *
    * @param id String The id of the record being shredded.
    * @param nodeWithLabel The XML node to be shredded.
    * @return (List[Triple], List[NodeWithLabels]  The complete triples and XML
    *         child nodes that are derived from the given node.
    */
  private def shredNode(id: String, nodeWithLabel: NodeWithLabel):
    (List[Triple], List[NodeWithLabel]) = {

    val node = nodeWithLabel.node
    val label = nodeWithLabel.label

    // Map attributes to Triples.
    val attributes: List[Triple] = node.attributes.flatMap(attr => {
      val attrLabel = s"${label}.@${attr.key}"
      attr.value.map(v => Triple(id, attrLabel, v.text.toString))
    }).toList

    // Map xmlns to Triple.
    val namespaces: List[Triple] =
      if(node.namespace.isEmpty) List()
      else {
        val attribute = s"${label}.@xmlns"
        List(Triple(id, attribute, node.namespace))
      }

    // Separate children into text nodes and xml (i.e. non-text) nodes.
    val (textNodes: List[Text], xmlNodes: List[Node]) =
      node.child.partition(_.isInstanceOf[Text])

    // Map text nodes Triples.
    val text: List[Triple] = textNodes.map(t => {
      val attribute = s"${label}.text()"
      Triple(id, attribute, t.text.toString)
    })

    // Map xml nodes to NodeWithLabels.
    val newNodes: List[NodeWithLabel] = xmlNodes.map(child => {
      val childLabel = s"${label}.${labelWithPrefix(child)}"
      NodeWithLabel(childLabel, child)
    })

    // Combine attributes and text, which are both complete triples.
    val newTriples = attributes ::: namespaces ::: text

    (newTriples, newNodes)
  }

  // Get the XML label. Include prefix if one is present.
  def labelWithPrefix(node: Node): String =
    // We have to use null here b/c it can be returned by scala.xml.Node
    if(node.prefix == null) node.label
    else s"${node.prefix}:${node.label}"
}

// An XML node plus its label
case class NodeWithLabel(label: String,
                         node: Node)

case class Triple(id: String,
                  attribute: String,
                  value: String)
