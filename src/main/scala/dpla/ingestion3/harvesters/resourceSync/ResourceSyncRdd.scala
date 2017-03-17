package dpla.ingestion3.harvesters.resourceSync

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}
import scala.collection.mutable.Stack
/**
  * Created by scott on 3/17/17.
  */

class ResourceSyncPartition(rddId: Int, idx: Int, val resourceUrl: String) extends Partition {
  override def index: Int = 0
}

class ResourceSyncRdd(resourceListUrl: String, sc: SparkContext) extends RDD[String](sc, Nil) {
  override protected def getPartitions: Array[Partition] = {
    Array[Partition](new Partition {
      override def index: Int = 0
    })
  }

  /**
    * Hard coded for testing purposes
    *
    * @param part
    * @param context
    * @return
    */
  override def compute(part: Partition, context: TaskContext): Iterator[String] = {
    new ResourceSyncIterator(Stack("https://hyphy.demo.hydrainabox.org/resourcelist"))
  }
}
