package dpla.ingestion3.preMappingReports

import org.json4s._

import scala.xml.Node

sealed trait Shred

case class Triple(id: String,
                  attribute: String,
                  value: String) extends Shred

// An XML node plus its label
case class NodeWithLabel(label: String,
                         node: Node) extends Shred

// A JValue plus its label
case class JValueWithLabel(label: String,
                           jValue: JValue) extends Shred