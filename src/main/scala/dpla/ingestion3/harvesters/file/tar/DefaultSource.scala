package dpla.ingestion3.harvesters.file.tar

import java.io._
import java.net.URI
import java.util.zip.GZIPInputStream

import dpla.ingestion3.harvesters.file.tar.DefaultSource.{SerializableConfiguration, _}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.{BinaryType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.tools.bzip2.CBZip2InputStream
import org.apache.tools.tar.TarInputStream

import scala.util.Try

/**
  * This is a DataSource that can handle Tar files in a number of formats: tar.gz, tar.bz2, or just .tar.
  *
  * It figures out which type it is based on the extension (.tgz and .tbz2 are also valid).
  *
  * Use it like:  val df = spark.read.format("dpla.ingestion3.harvesters.file.tar").load(infile)
  *
  */
class DefaultSource extends FileFormat with DataSourceRegister {

  def shortName(): String = "tar"

  /**
    * Writing not implemented yet. Not sure we need it.
    *
    */
  override def prepareWrite(
                             sparkSession: SparkSession,
                             job: Job,
                             options: Map[String, String],
                             dataSchema: StructType
                           ): OutputWriterFactory =
    throw new UnsupportedOperationException("Writing not implemented.")


  /**
    * Schema is always a tuple of the name of the tarfile, the full path of the entry, and the bytes of the entry.
    *
    */
  override def
  inferSchema(sparkSession: SparkSession, options: Map[String, String], files: Seq[FileStatus]): Option[StructType] =
    Some(StructType(Seq(
      StructField("tarname", StringType, nullable = false),
      StructField("filename", StringType, nullable = false),
      StructField("data", BinaryType, nullable = false)
    )))

  /**
    * Not even thinking about how to make this data splittable. In many cases this will handle, it's definitely not.
    *
    */
  override def isSplitable(
                            sparkSession: SparkSession,
                            options: Map[String, String],
                            path: Path): Boolean = false


  override def buildReader(
                            spark: SparkSession,
                            dataSchema: StructType,
                            partitionSchema: StructType,
                            requiredSchema: StructType,
                            filters: Seq[Filter],
                            options: Map[String, String],
                            hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {

    //We avoid closing over the non-serializable HadoopConf by wrapping it in something serializable and
    //broadcasting it. Really, Hadoop? It's like, a hashtable. You could save us a lot of trouble by making it
    //Serializable. But we need a Configuration to laod the data using Hadoop's FileSystem class, which means we
    //should be able to read files from everywhere Spark can.
    val broadcastedConf = spark.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    //this is a function that Spark will call on every file given as input. It's going to be run on a worker node,
    //not the master, so we have to take a little care not to close over anything that's not serializable.
    (file: PartitionedFile) => {

      //this contains the stream if it's something we can read, or None if not.
      val tarInputStreamOption: Option[TarInputStream] = loadStream(broadcastedConf, file)
      tarInputStreamOption match {

        //we got some data to iterate over
        case Some(tarInputStream) =>
          iterateFiles(file.filePath, tarInputStream)

        //The default case cowardly fails. Possibly a directory with
        //other files was passed, so we don't want to blow up
        case None =>
          Iterator.empty
      }
    }
  }
}

object DefaultSource {

  def iter(tarInputStream: TarInputStream): Stream[TarResult] = {
    Option(tarInputStream.getNextEntry) match {
      case None =>
        Stream.empty

      case Some(entry) =>

        val result =
          if (entry.isDirectory)
            TarResult(
              UTF8String.fromString(entry.getName),
              None,
              isDirectory = true
            )
          else
            TarResult(
              UTF8String.fromString(entry.getName),
              Some(IOUtils.toByteArray(tarInputStream, entry.getSize)),
              isDirectory = false
            )

        result #:: iter(tarInputStream)
    }
  }


  def iterateFiles(filePath: String, tarInputStream: TarInputStream): Iterator[InternalRow] = {

    /*
      InternalRow doesn't want a plain old String because that doesn't enforce an encoding on disk. Also, UTF8String
      models the data as an array of bytes internally, which is probably denser than native Strings.

      InternalRow generally deals in these wrapped, more primitive representations than the standard library. We only
      happen to be using one of these primative wrappers here, but there are more.
     */
    val filePathUTF8 = UTF8String.fromString(filePath)

    /*
      This helper method lazily traverses the TarInputStream.
      Can't use @tailrec here because the compiler can't recognize it as tail recursive, but this won't blow the stack.
     */
    iter(tarInputStream)
      .filterNot(_.isDirectory) //we don't care about directories
      .map((result: TarResult) =>
      InternalRow(
        UTF8String.fromString(filePath),
        result.entryPath,
        result.data.getOrElse(Array[Byte]())
      )
    ).iterator
  }

  def loadStream(broadcastedConf: Broadcast[SerializableConfiguration], file: PartitionedFile):
  Option[TarInputStream] = {
    val hadoopConf = broadcastedConf.value.value
    val fs = FileSystem.get(hadoopConf)
    val path = new Path(new URI(file.filePath))
    file.filePath match {
      case name if name.endsWith("gz") || name.endsWith("tgz") =>
        Some(new TarInputStream(new GZIPInputStream(fs.open(path))))
      case name if name.endsWith("bz2") || name.endsWith("tbz2") =>
        //skip the "BZ" header added by bzip2.
        val fileStream = fs.open(path)
        fileStream.skip(2)
        Some(new TarInputStream(new CBZip2InputStream(fileStream)))
      case name if name.endsWith("tar") =>
        Some(new TarInputStream(fs.open(path)))
      case _ => None //We don't recognize the extension.
    }
  }


  /**
    * This is one of those things that seems to exist in a lot of Hadoop ecosystem projects that Hadoop should just fix.
    * Basically this is just a wrapper that allows a Hadoop Configuration object to be sent over the wire.
    * Needed so that workers can find their files on whatever filesystem is passed in (HDFS, S3, etc.)
    *
    * @param value the Hadoop Configuration object to be sent.
    */
  class SerializableConfiguration(@transient var value: Configuration) extends Serializable {
    private def writeObject(out: ObjectOutputStream): Unit =
      Try {
        out.defaultWriteObject()
        value.write(out)
      } getOrElse (
        (e: Exception) => throw new IOException("Unable to read config from input stream.", e)
      )


    private def readObject(in: ObjectInputStream): Unit =
      Try {
        value = new Configuration(false)
        value.readFields(in)
      } getOrElse (
        (e: Exception) => throw new IOException("Unable to read config from input stream.", e)
      )
  }

  /**
    * Case class for holding the result of a single tar entry iteration.
    *
    * @param entryPath   Path of the entry in the tar file.
    * @param data        Binary data in the entry
    * @param isDirectory If the entry represents a directory.
    */
  case class TarResult(entryPath: UTF8String, data: Option[Array[Byte]], isDirectory: Boolean)

}
