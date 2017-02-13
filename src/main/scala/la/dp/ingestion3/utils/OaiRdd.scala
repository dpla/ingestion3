package la.dp.ingestion3.utils

import la.dp.ingestion3.OaiQueryUrlBuilder
import la.dp.ingestion3.harvesters.OaiFeedTraversable
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
  * Created by scott on 2/10/17.
  */
class OaiRdd(sc: SparkContext,
             params: Map[String, String],
             urlBuilder: OaiQueryUrlBuilder) extends RDD[Map[String,String]](sc, Nil) {

  /**
    *
    * @return
    */
  override protected def getPartitions: Array[Partition] = {
    Array[Partition](new Partition {
      override def index: Int = 0
    })
  }

  /**
    *
    * @param split
    * @param context
    */
  override def compute(split: Partition, context: TaskContext): Iterator[Map[String,String]] = {
    new OaiFeedTraversable(params, urlBuilder).toIterator
    // record.map { case(id, data) => fileIO.writeFile(id, data) }
  }

}
