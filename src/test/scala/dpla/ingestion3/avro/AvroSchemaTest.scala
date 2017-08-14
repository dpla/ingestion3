package dpla.ingestion3.avro

import dpla.ingestion3.utils.FlatFileIO
import org.apache.avro.Schema
import org.scalatest.{BeforeAndAfter, FlatSpec}

class AvroSchemaTest  extends FlatSpec with BeforeAndAfter {

  val schemaFiles = Seq("/avro/IndexRecord_MAP3.avsc", "/avro/IndexRecord_MAP4.avsc", "/avro/MAPRecord.avsc", "/avro/OriginalRecord.avsc")

  "an Avro schema file" should "parse" in {
    for (file <- schemaFiles) {
      val schemaString = new FlatFileIO().readFileAsString(file)
      val schema = new Schema.Parser().parse(schemaString)
    }
  }
}
