package la.dp.ingestion3.utils

import java.io.File
import java.nio.file.Files
import java.nio.file.StandardOpenOption.{CREATE, TRUNCATE_EXISTING}

import org.apache.spark.rdd.RDD

/**
  * Basic FileIO ops
  *
  */
class FlatFileIO extends FileIO {
  /**
    * Save the file to disk
    */
  def writeFile(record: String, file: String): Unit = {
    val outFile = new File(file).toPath
    Files.write(outFile, record.getBytes("utf8"), CREATE, TRUNCATE_EXISTING)
  }
}

/**
  * FileIO implementation using SequenceFiles
  *
  */
class SeqFileIO extends  FileIO {
  /**
    * Saves data to SequenceFile
    *
    * TODO: this is not how seq files should be written, I need the entirety
    * of what needs to be written to disk, not individual files
    *
    * Either I aggregate them outside of OaiHarvester or after harvesting all
    * records into single files I use those files to create the SeqFile
    *
    * @param record data to save
    * @param outFile
    */
  def writeFile(record: String, outFile: String): Unit = {
//    val inputData: RDD[(String, Array[Byte])] = sc.sequenceFile[String, Array[Byte]](args(0))
//    val outputData = inputData.map(record => (record._1, process(record._2)))
//    val key = new File(outFile).getName
//    val mapRdd: RDD[(String, String)] = new RDD (key,record)
//    mapRdd.saveAsSequenceFile(outFile,
//    Some(classOf[org.apache.hadoop.io.compress.DefaultCodec]))
  }
}

trait FileIO {
  def writeFile(record: String, outFile: String): Unit
}