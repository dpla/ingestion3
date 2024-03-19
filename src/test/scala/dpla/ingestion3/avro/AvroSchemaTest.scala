package dpla.ingestion3.avro

import dpla.ingestion3.utils.FlatFileIO
import org.apache.avro.Schema
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class AvroSchemaTest  extends AnyFlatSpec with BeforeAndAfter {

  val schemaFiles: Seq[String] = Seq("/avro/IndexRecord_MAP3.avsc", "/avro/IndexRecord_MAP4.avsc", "/avro/MAPRecord.avsc", "/avro/OriginalRecord.avsc")

  "an Avro schema file" should "parse" in {
    for (file <- schemaFiles) {
      val schemaString = new FlatFileIO().readFileAsString(file)
      val schema = new Schema.Parser().parse(schemaString)
    }
  }
}
