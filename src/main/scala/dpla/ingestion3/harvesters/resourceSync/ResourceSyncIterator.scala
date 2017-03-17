package dpla.ingestion3.harvesters.resourceSync

import scala.collection.mutable.Stack
import scala.xml._

/**
  * Expects an endpoint
  *
  * @param
  */
class ResourceSyncIterator ( resourceLists: Stack[String]) extends Iterator[String] {
  private[this] val buffer = new Stack[String]

  override def hasNext: Boolean = {
    if (buffer.isEmpty) fillBuffer
    buffer.nonEmpty
  }

  override def next(): String = {
    if (buffer.isEmpty) fillBuffer
    buffer.pop
  }

  /**
    *
    * @return
    */
  private[this] def fillBuffer(): Unit = {
    if (resourceLists.isEmpty)
      return

    val resourcePage = resourceLists.pop
    val rsp = XML.load(resourcePage)
    val locs = rsp \\ "loc"
    val resourceUrls = locs.map(_.text)
    buffer.pushAll(resourceUrls)
  }
}
