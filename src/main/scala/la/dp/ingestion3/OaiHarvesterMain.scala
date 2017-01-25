package la.dp.ingestion3

import la.dp.ingestion3.harvesters.OaiHarvester
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.SequenceFile.{Reader, Writer}
import org.apache.hadoop.io.{SequenceFile, Text}

import scala.xml.XML
/**
  * Created by scott on 1/21/17.
  */
object OaiHarvesterMain extends App with OaiHarvester {

  override def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    val fs: FileSystem = FileSystem.getLocal(conf)
    val seqFilePath: Path = new Path("pa2.seq");

    // Utils.printSeqFile(fs,seqFilePath,conf)
    println(getHarvestCount(fs,seqFilePath,conf))
  }

  /**
    * Execute the harvest
    */
  def runHarvest(fs: FileSystem, seqFilePath: Path, conf: Configuration) = {
    val writer: SequenceFile.Writer = SequenceFile.createWriter(conf,
      Writer.file(seqFilePath), Writer.keyClass(new Text().getClass),
      Writer.valueClass(new Text().getClass))

    val endpoint = "aggregator.padigital.org/oai"
    val metadataPrefix = "oai_dc"
    val verb = "ListRecords"
    var resumptionToken: String = ""
    val start = System.currentTimeMillis()

    try {
      do {
        resumptionToken = harvest(writer, resumptionToken, endpoint, metadataPrefix, verb)
      } while (resumptionToken.nonEmpty)

      val end = System.currentTimeMillis()

      println("Runtime: " + (end - start) + "ms.")
    } catch {
      case e: Exception => {
        println(e.toString)
      }
    } finally {
      writer.close()
    }
  }

  /**
    *
    */
  def getHarvestCount(fs: FileSystem, seqFilePath: Path, conf: Configuration): Long = {
    val reader: Reader = new Reader(fs, seqFilePath, conf)
    var count = 0

    val k: Text = new Text()
    val v: Text = new Text()

    try {
      while (reader.next(k, v)) {
        val xml = XML.loadString(v.toString)
        // println(xml.child)
        count += 1
      }
    } finally {
      reader.close()
    }
    count
  }
}
