package la.dp.ingestion3.utils

import java.io.File
import java.nio.file.Files
import java.nio.file.StandardOpenOption.{CREATE, TRUNCATE_EXISTING}

import org.apache.avro.Schema
import org.apache.avro.file.{CodecFactory, DataFileWriter}
import org.apache.avro.generic.{GenericDatumWriter, GenericRecordBuilder}

/**
  * Basic FileIO ops
  *
  */
class FlatFileIO extends FileIO {
  /**
    * Save the file to disk
    *
    * @param record String
    * @param outputFile String
    */
  def writeFile(record: String, outputFile: String): Unit = {
    val outFile = new File(outputFile).toPath
    Files.write(outFile, record.getBytes("utf8"), CREATE, TRUNCATE_EXISTING)
  }
}

/**
  * FileIO implementation using SequenceFiles
  *
  */
class AvroFileIO extends FileIO {

  val avsc =
  """
    |{
    | "type" : "record",
    | "name" : "oaiHarvester",
    | "fields": [
    |   {
    |     "name": "data",
    |     "type" : "string"
    |   }
    | ]
    |}
  """.stripMargin

  val schema = new Schema.Parser().parse(avsc)
  val builder = new GenericRecordBuilder(schema)

  /**
    * Saves data to AvroFile
    *
    * TODO figure this sheet out
    *   Commented out for testing
    * @param record data to save
    */
  def writeFile(record: String): Unit = {
//    val writer = new DataFileWriter[Object](new GenericDatumWriter[Object]())
//    writer.setCodec(CodecFactory.snappyCodec())
//    builder.set("data", record)
//    writer.appendTo(builder.build())
//    writer.close()
  }

  /**
    *
    * @param record
    * @param outputFile
    */
  override def writeFile(record: String, outputFile: String): Unit = {
  }
}

trait FileIO {
  def writeFile(record: String, outputFile: String): Unit
}