package dpla.ingestion3.utils

import dpla.ingestion3.harvesters.OaiQueryUrlBuilder
import dpla.ingestion3.harvesters.oai.OaiFeedIterator
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

/**
  * Created by scott on 2/10/17.
  */
class OaiRdd(sc: SparkContext,
             params: Map[String, String],
             urlBuilder: OaiQueryUrlBuilder) extends RDD[(String,String)](sc, Nil) {

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
  override def compute(split: Partition, context: TaskContext): Iterator[(String,String)] = {
    // new OaiFeedTraversable(params, urlBuilder).toIterator

    // Replace Traversable with Iterator
    new OaiFeedIterator(params, urlBuilder)
  }
}
