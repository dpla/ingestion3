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
  * FileIO implementation using Avro files
  *
  * @param avschema String
  *                 The schema to serialize to
  * @param outputFile File
  *                   The Avro file destination
  */
class AvroFileIO (avschema: String, outputFile: File) extends FileIO {

  // Create and configure the writer
  val schema = new Schema.Parser().parse(avschema)
  val writer = new DataFileWriter[Object](new GenericDatumWriter[Object]())
  val builder = new GenericRecordBuilder(schema)
  writer.setCodec(CodecFactory.snappyCodec())
  writer.create(schema, outputFile)

  /**
    * Saves data to AvroFile
    *
    * TODO figure this sheet out
    *   Commented out for testing
    * @param record data to save
    */
  def writeFile(record: String): Unit = {
    builder.set("body", record)
    writer.append(builder.build())
  }

  /**
    *
    * @param record
    * @param outputFile
    */
  override def writeFile(record: String, outputFile: String): Unit = {
  }

  /**
    * Close the writer
    *
    */
  def close: Unit = {
    writer.close()
  }
}

/**
  *
  */
trait FileIO {
  def writeFile(record: String, outputFile: String): Unit
}