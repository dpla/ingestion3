package dpla.ingestion3.utils

import dpla.ingestion3.harvesters.ResourceSyncUrlBuilder
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.xml._
import scalaj.http.Http


class ResourceSyncPartition(val resourceUrl: String) extends Partition {
  override def index: Int = 0
}

class ResourceSyncRdd (resourceListUrl: String, sc: SparkContext) extends RDD[String](sc, Nil) {

  override protected def getPartitions: Array[ResourceSyncPartition] = {
    val rsp = XML.load(resourceListUrl)
    val resourceUrls = rsp \\ "url" \\ "loc"
    resourceUrls.map(r => new ResourceSyncPartition(r.text)).toArray
  }

  override def compute(part: Partition, context: TaskContext): Iterator[String] = {
    val p = part.asInstanceOf[ResourceSyncPartition]
    val resourceUrl = p.resourceUrl

    new Iterator[String] {
      var nextURL = ""
      override def hasNext: Boolean = {
        nextURL = resourceUrl
        resourceUrl.nonEmpty
      }

      /**
        * this needs some more thought but should be functional
        * @return
        */
      override def next(): String = {
        val urlBuilder = new ResourceSyncUrlBuilder()
        val url = urlBuilder.buildQueryUrl(Map("endpoint"->nextURL))

        Http(url.toString).toString
      }
    }
  }
}
