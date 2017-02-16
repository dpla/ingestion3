package la.dp.ingestion3.harvesters

import la.dp.ingestion3.OaiQueryUrlBuilder
import org.apache.log4j.LogManager

import scala.collection.mutable.ArrayBuffer


class OaiFeedIterator(params: Map[String,String], urlBuilder: OaiQueryUrlBuilder)
  extends Iterator[(String,String)] {

  override def hasNext: Boolean = {
    if ( buffer.isEmpty) fillBuffer
    buffer.nonEmpty
  }

  override def next(): (String, String) = {
    if ( buffer.isEmpty) fillBuffer
    buffer.head
  }

  private[this] val logger = LogManager.getLogger(this.getClass)

  private[this] val buffer = new ArrayBuffer[(String,String)]

  private[this] var resumptionToken = ""

  private[this] def fillBuffer: Unit = ??? //todo
}
