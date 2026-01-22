package dpla.ingestion3.utils

import dpla.ingestion3.harvesters.file.FileFilters

import java.io.{BufferedWriter, File, FileWriter}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.storage.StorageLevel

import scala.xml.XML

case class MergeLogs(
    // base DF
    basePath: Option[String] = None,
    baseCount: Option[Long] = None,
    baseUniqueCount: Option[Long] = None,
    baseDuplicateCount: Option[Long] = None,
    // delta DF
    deltaPath: Option[String] = None,
    deltaCount: Option[Long] = None,
    deltaUniqueCount: Option[Long] = None,
    deltaDuplicateCount: Option[Long] = None,
    // merged DF
    mergePath: Option[String] = None,
    mergeNew: Option[Long] = None,
    mergeUpdate: Option[Long] = None,
    mergeTotal: Option[Long] = None,
    mergeTotalExpected: Option[Long] = None,
    // delete df
    deletePath: Option[String] = None,
    deleteActual: Option[Long] = None,
    deleteInFile: Option[Long] = None,
    deleteValid: Option[Long] = None,
    deleteInvalid: Option[Long] = None,
    // final
    finalDfActual: Option[Long] = None,
    output: Option[String] = None,
    logs: Option[String] = None
)

/** */
object NaraMergeUtil {

  /** Accepts two NARA harvest DataFrames and merges them
    *
    * Expects five parameters
    *   - Path to base/initial data
    *   - Path to delta harvest
    *   - Path to deletes XML files
    *   - Path to save merged output
    *   - Spark master
    *
    */
  def main(args: Array[String]): Unit = {
    if (args.length != 5) {
      println("Incorrect number of parameters. Terminating run.")
      System.exit(1)
    }

    val basePath = args(0)
    val deltaPath = args(1)
    val deletesPath = args(2)
    val outputPath = args(3)
    val sparkMaster = args(4)
    val logsPath = outputPath + "/_LOGS/"

    println(s"""
        | Running NARA data merge
        | - basePath = $basePath
        | - deltaPath = $deltaPath
        | - deletesPath = $deletesPath
        | - outputPath = $outputPath
        | - logsPath = $logsPath
        | - sparkMaster = $sparkMaster
        """.stripMargin)

    val baseConf = new SparkConf()
      .setAppName(s"Nara merge utility")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "200")

    val sparkConf = Option(sparkMaster) match {
      case Some(m) => baseConf.setMaster(m)
      case None    => baseConf
    }

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    import spark.implicits._

    // Read and cache harvest data files to avoid re-reading during multiple operations
    val baseHarvestDf: DataFrame = spark.read.format("avro").load(basePath)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val deltaHarvestDf: DataFrame = spark.read.format("avro").load(deltaPath)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    // Counts for logging
    val baseCount = baseHarvestDf.count()
    val deltaCount = deltaHarvestDf.count()

    // Dedup harvest DataFrames
    // Returns (duplicateCount, dedupedDataFrame) - no longer collects IDs to driver
    val baseDuplicates = dropDuplicates(baseHarvestDf, spark)
    val baseDuplicateCount = baseDuplicates._1
    val baseHarvestDedupDf = baseDuplicates._2
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val baseDedupCount = baseHarvestDedupDf.count() // for logging

    val deltaDuplicates = dropDuplicates(deltaHarvestDf, spark)
    val deltaDuplicateCount = deltaDuplicates._1
    val deltaHarvestDedupDf = deltaDuplicates._2
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val deltaDedupCount = deltaHarvestDedupDf.count() // for logging

    // Create temp views of DataFrames for update DF
    deltaHarvestDedupDf.createOrReplaceTempView("delta")
    baseHarvestDedupDf.createOrReplaceTempView("base")

    // get records that exist in base that will be updated by new version in delta
    val updateDF =
      spark.sql("SELECT base.* FROM base JOIN delta on base.id = delta.id")
    updateDF.createOrReplaceTempView("update")
    val mergeUpdateCount = updateDF.count() // for logging

    // Get new records added by delta
    val newRecords = spark.sql(
      "SELECT delta.* FROM delta " +
        "LEFT JOIN base ON delta.id = base.id " +
        "WHERE base.id IS NULL"
    )
    val newRecordsCount = newRecords.count()

    // merge base and delta data sets
    val mergedDf = spark
      .sql(
        "SELECT base.* FROM base " +
          "LEFT JOIN update ON base.id = update.id " +
          "WHERE update.id IS NULL"
      ) // drop records from previous harvest that exist in the update
      .union(deltaHarvestDedupDf) // add updates and new records
      .toDF()
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val mergeTotalCount = mergedDf.count() // for logging

    // process deletes - use Spark DataFrame instead of collecting to driver memory
    val deletesDf = getIdsToDeleteDf(deletesPath, spark)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val deleteInFileCount = deletesDf.count()

    mergedDf.createOrReplaceTempView("merged")
    deletesDf.createOrReplaceTempView("deletes")

    // Use Spark SQL joins instead of collecting IDs to driver memory
    val validDeletes = spark.sql(
      "SELECT merged.id FROM merged JOIN deletes ON merged.id = deletes.id"
    ).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val invalidDeletes = spark.sql(
      "SELECT deletes.id FROM deletes LEFT JOIN merged ON deletes.id = merged.id WHERE merged.id IS NULL"
    )

    // Use left_anti join instead of collecting delete IDs to driver memory
    // This keeps all processing distributed and avoids OOM errors
    val mergedWithDeletesDf = mergedDf.join(
      validDeletes.select("id"),
      Seq("id"),
      "left_anti"  // Returns rows from left that have no match in right
    ).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val mergedWithDeletesCount = mergedWithDeletesDf.count()

    import org.apache.spark.sql.functions._
    val opsDf: DataFrame = newRecords
      .select("id")
      .withColumn("operation", lit("insert"))
      .union(updateDF.select("id").withColumn("operation", lit("update")))
      .union(validDeletes.select("id").withColumn("operation", lit("delete")))
      .union(
        invalidDeletes
          .select("id")
          .withColumn("operation", lit("invalid delete"))
      )

    // Cache counts that are used multiple times
    val validDeletesCount = validDeletes.count()
    val invalidDeletesCount = invalidDeletes.count()

    val logs = MergeLogs(
      // base
      basePath = Some(basePath),
      baseCount = Some(baseCount),
      baseUniqueCount = Some(baseDedupCount),
      baseDuplicateCount = Some(baseDuplicateCount),
      // delta
      deltaPath = Some(deltaPath),
      deltaCount = Some(deltaCount),
      deltaUniqueCount = Some(deltaDedupCount),
      deltaDuplicateCount = Some(deltaDuplicateCount),
      // merge
      mergePath = Some(outputPath),
      mergeNew = Some(newRecordsCount),
      mergeUpdate = Some(mergeUpdateCount),
      mergeTotal = Some(mergeTotalCount),
      mergeTotalExpected = Some(newRecordsCount + baseDedupCount),
      // delete
      deletePath = Some(deletesPath),
      deleteActual = Some(mergeTotalCount - mergedWithDeletesCount),
      deleteInFile = Some(deleteInFileCount),
      deleteValid = Some(validDeletesCount),
      deleteInvalid = Some(invalidDeletesCount),
      // final
      finalDfActual = Some(mergedWithDeletesCount),
      output = Some(outputPath),
      logs = Some(logsPath)
    )

    val summary = getLogs(logs)

    // Write merged data with deletes
    new File(outputPath).mkdirs()
    mergedWithDeletesDf.write
      .mode(SaveMode.Overwrite)
      .format("avro")
      .save(outputPath)

    // print summary log
    println(summary)

    // Write summary log to text file
    new File(logsPath).mkdirs()
    val file = new File(logsPath + "/_SUMMARY.txt")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(summary)
    bw.close()

    // Write operations CSV [update, insert, delete, invalid delete]
    val opsFile = logsPath + "/ops/"
    opsDf.write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv(opsFile)

    // Clean up cached DataFrames to free memory
    baseHarvestDf.unpersist()
    deltaHarvestDf.unpersist()
    baseHarvestDedupDf.unpersist()
    deltaHarvestDedupDf.unpersist()
    mergedDf.unpersist()
    deletesDf.unpersist()
    validDeletes.unpersist()
    mergedWithDeletesDf.unpersist()

    spark.stop()
  }

  /** Collect IDs from file(s) to be deleted from NARA harvest as a DataFrame.
    * This distributed approach avoids OOM errors when dealing with large
    * numbers of delete IDs.
    *
    * @param path
    *   String Path to file or folder that defines which NARA IDs should be deleted
    * @param spark
    *   SparkSession for creating DataFrame
    * @return
    *   DataFrame with single "id" column containing IDs to delete
    */
  def getIdsToDeleteDf(path: String, spark: SparkSession): DataFrame = {
    import spark.implicits._

    val file = new File(path)
    if (!file.exists())
      return spark.emptyDataFrame.withColumn("id", col("id").cast("string"))

    val files =
      if (file.isDirectory) file.listFiles(FileFilters.xmlFilter).sorted
      else Array(file)

    // Parse XML files and create DataFrame - keeps processing distributed
    val ids = files
      .flatMap(file => {
        val xml = XML.loadFile(file)
        (xml \\ "naId").map(_.text)
      })
      .distinct

    // Convert to DataFrame for distributed processing
    ids.toSeq.toDF("id")
  }

  /** @deprecated Use getIdsToDeleteDf instead to avoid OOM errors with large delete sets
    *
    * Collect IDs from file(s) to be deleted from NARA harvest. This does not
    * currently support adding records back which had previously been deleted.
    *
    * @param path
    *   String Path to file or folder the defines which NARA IDs should be
    *   deleted
    * @return
    *   Seq[String] IDs to delete
    */
  @deprecated("Use getIdsToDeleteDf instead to avoid OOM errors", "2026.01")
  def getIdsToDelete(path: String): Seq[String] = {
    val file = new File(path)
    if (!file.exists())
      return Seq() // if deletes do not exist do nothing
    val files =
      if (file.isDirectory) file.listFiles(FileFilters.xmlFilter).sorted
      else Array(file)
    files
      .flatMap(file => {
        val xml = XML.loadFile(file)
        (xml \\ "naId").map(_.text)
      })
      .distinct
      .toSeq
  }

  /** Drop duplicate records from DataFrame and return the count of duplicates.
    * This optimized version does not collect duplicate IDs to driver memory,
    * avoiding OOM errors with large datasets.
    *
    * @param df
    *   DataFrame to deduplicate
    * @param spark
    *   SparkSession
    * @return
    *   Tuple of (duplicate count, deduplicated DataFrame)
    */
  private def dropDuplicates(
      df: DataFrame,
      spark: SparkSession
  ): (Long, DataFrame) = {
    df.createOrReplaceTempView("duplicates")

    // Count duplicates without collecting to driver memory
    val duplicateCount = spark.sql(
      "SELECT COUNT(*) as cnt FROM (" +
        "SELECT duplicates.id " +
        "FROM duplicates " +
        "GROUP BY duplicates.id HAVING COUNT(duplicates.id) > 1" +
        ") as dups"
    ).first().getLong(0)

    if (duplicateCount > 0) {
      // drop duplicate records from dataframe
      (duplicateCount, df.dropDuplicates("id"))
    } else {
      (0L, df)
    }
  }

  /**
   * Write logs
   */
  private def getLogs(logs: MergeLogs): String = {
    val logText =
      s"""
        | Base
        | ----------
        | path: ${logs.basePath.getOrElse("Unknown")}
        | record count ${Utils.formatNumber(logs.baseCount.getOrElse(-1))}
        | duplicate count ${Utils.formatNumber(
          logs.baseDuplicateCount.getOrElse(-1)
        )}
        | unique count ${Utils.formatNumber(logs.baseUniqueCount.getOrElse(-1))}
        |
        | Delta
        | -----------
        | path: ${logs.deltaPath.getOrElse("Unknown")}
        | record count: ${Utils.formatNumber(logs.deltaCount.getOrElse(-1))}
        | duplicate count: ${Utils.formatNumber(
          logs.deltaDuplicateCount.getOrElse(-1)
        )}
        | unique count: ${Utils.formatNumber(
          logs.deltaUniqueCount.getOrElse(-1)
        )}
        |
        | Merged
        | -----------
        | path: ${logs.mergePath.getOrElse("Unknown")}
        | new: ${Utils.formatNumber(logs.mergeNew.getOrElse(-1))}
        | update: ${Utils.formatNumber(logs.mergeUpdate.getOrElse(-1))}
        | total [actual]: ${Utils.formatNumber(logs.mergeTotal.getOrElse(-1))}
        | total [expected]: ${Utils.formatNumber(
          logs.mergeTotalExpected.getOrElse(-1)
        )} = ${Utils.formatNumber(
          logs.baseUniqueCount.getOrElse(-1)
        )} (base) + ${Utils.formatNumber(logs.mergeNew.getOrElse(-1))} (new)
        |
        | Delete
        | ------------
        | path: ${logs.deletePath.getOrElse("Unknown")}
        | ids to delete specified at path: ${Utils.formatNumber(
          logs.deleteInFile.getOrElse(0)
        )}
        | invalid deletes (IDs not in merged dataset): ${Utils.formatNumber(
          logs.deleteInvalid.getOrElse(-1)
        )}
        | valid deletes (IDs in merged dataset): ${Utils.formatNumber(
          logs.deleteValid.getOrElse(-1)
        )}
        | actual removed (merged total - total after deletes): ${Utils
          .formatNumber(logs.deleteActual.getOrElse(0))}
        |
        | Final
        | -----
        | total [actual]: ${Utils.formatNumber(
          logs.finalDfActual.getOrElse(0)
        )} = ${Utils.formatNumber(
          logs.mergeTotal.getOrElse(-1)
        )} (merge actual) - ${Utils.formatNumber(
          logs.deleteActual.getOrElse(0)
        )} (delete actual)
        | output: ${logs.output.getOrElse("Unknown")}
        | logs: ${logs.logs.getOrElse("Unknown")}
      """.stripMargin

    logText
  }
}
