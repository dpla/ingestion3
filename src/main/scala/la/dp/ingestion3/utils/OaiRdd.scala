package la.dp.ingestion3.utils

import la.dp.ingestion3.OaiQueryUrlBuilder
import la.dp.ingestion3.harvesters.OaiFeedTraversable
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
  * Created by scott on 2/10/17.
  */

abstract class OaiPartition (rddId: Int, id: String, data: String) extends Partition {}

class OaiRdd(sc: SparkContext,
             params: Map[String, String],
             urlBuilder: OaiQueryUrlBuilder) extends RDD[Any](sc, Nil) {

  /**
    *
    * @return
    */
  override protected def getPartitions: Array[Partition] = {
    val partition = new ArrayBuffer[OaiPartition]
    partition.toArray
  }

  /**
    *
    * @param split
    * @param context
    */
  override def compute(split: Partition, context: TaskContext) = {
    val s = split.isInstanceOf[OaiPartition]

    new OaiFeedTraversable(params, urlBuilder).toIterator
    // record.map { case(id, data) => fileIO.writeFile(id, data) }
  }

}
