package dpla.ingestion3.utils

import org.apache.spark.Partition

class ResourceSyncPartition(rddId: Int, idx: Int, val resourceListUrl: String) extends Partition {}

class RsRdd {

}
