package dpla.ingestion3.harvesters.resourceSync

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

/**
  *
  * @param idx
  * @param resourceUrls
  */
class ResourceSyncPartition(val idx: Int, val resourceUrls: Seq[String]) extends Partition {
  override def index: Int = idx
}

/**
  *
  * @param resourceList
  * @param sc
  */
class ResourceSyncRdd(resourceList: Seq[String], sc: SparkContext) extends RDD[(String,String)](sc, Nil) {
  /**
    *
    * @return
    */
  override protected def getPartitions: Array[Partition] = {
    val groupSize = 10 // Arbitrary size
    // Split resourceList into X groups of 10
    val partitionedResourceLists = resourceList.grouped(groupSize).toList
    val partitions = new Array[Partition](partitionedResourceLists.size)

    for (i <- partitionedResourceLists.indices)
      partitions(i) = new ResourceSyncPartition(i, partitionedResourceLists(i))

    partitions
  }

  /**
    *
    * @param part
    * @param context
    * @return
    */
  override def compute(part: Partition, context: TaskContext): Iterator[(String,String)] = {
    val p = part.asInstanceOf[ResourceSyncPartition]
    val rsi = new ResourceSyncIterator()
    rsi.fillBuffer(p.resourceUrls)
    rsi
  }
}
