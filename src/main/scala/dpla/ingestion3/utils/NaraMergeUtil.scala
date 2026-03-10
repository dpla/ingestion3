package dpla.ingestion3.utils

import dpla.ingestion3.harvesters.file.FileFilters

import java.io.{BufferedWriter, File, FileWriter}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.col

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
      .set("spark.sql.shuffle.partitions", "400")
      .set("spark.default.parallelism", "200")
      .set("spark.memory.fraction", "0.7")
      .set("spark.memory.storageFraction", "0.3")

    val sparkConf = Option(sparkMaster) match {
      case Some(m) => baseConf.setMaster(m)
      case None    => baseConf
    }

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    // ---------------------------------------------------------------
    // ZERO-PERSISTENCE strategy for large NARA merges:
    //
    // Problem: With 18M+ base records containing large XML documents,
    // Spark's persist (even DISK_ONLY) causes either:
    //   - OOM from columnar serialization (MEMORY_AND_DISK)
    //   - 300+ GB of Kryo-serialized temp data (DISK_ONLY)
    //
    // Solution: Never persist the base DataFrame. Re-read from Avro
    // each time (fast from SSD: ~30-60s per scan of ~34 GB).
    // Only persist the small delta DataFrame.
    // Skip base dedup since it was already deduped in previous merge.
    //
    // Base scans required:
    //   1. count() - count base records (~30s)
    //   2. filter(isDeltaId).count() - count updates (~30s)
    //   3. filter(!isDeltaId).union(delta).write - merge+write (~60s)
    // Total: ~3 scans = ~2 minutes of base I/O
    // ---------------------------------------------------------------

    // Step 1: Process delta (small - typically hundreds of thousands of records)
    println("Reading delta harvest data...")
    val deltaHarvestDf: DataFrame = spark.read.format("avro").load(deltaPath)
    val deltaCount = deltaHarvestDf.count()
    println(s"Delta records: $deltaCount")

    // Dedup delta - write to temp Avro for efficient re-reads
    // (avoids keeping large XML records in Spark's in-memory cache)
    println("Deduplicating delta...")
    val deltaTmpPath = outputPath + "__tmp_delta_dedup"
    deltaHarvestDf.dropDuplicates("id").write
      .mode(SaveMode.Overwrite)
      .format("avro")
      .save(deltaTmpPath)
    val deltaHarvestDedupDf = spark.read.format("avro").load(deltaTmpPath)
    val deltaDedupCount = deltaHarvestDedupDf.count()
    val deltaDuplicateCount = deltaCount - deltaDedupCount
    println(s"Delta unique records: $deltaDedupCount (duplicates: $deltaDuplicateCount)")

    // Collect delta IDs into a broadcast variable for efficient filtering
    // Safe because delta is small (hundreds of thousands, not millions)
    val deltaIds: Set[String] = deltaHarvestDedupDf
      .select("id")
      .collect()
      .map(_.getString(0))
      .toSet
    val deltaIdsBroadcast = spark.sparkContext.broadcast(deltaIds)
    println(s"Broadcast ${deltaIds.size} delta IDs")

    // Step 2: Process base - NO persist, just re-read from Avro each time
    // The base was already deduped in the previous merge, so skip dedup.
    println("Counting base harvest records (direct Avro scan, no persist)...")
    val baseDedupCount = spark.read.format("avro").load(basePath).count()
    val baseCount = baseDedupCount
    val baseDuplicateCount = 0L
    println(s"Base records: $baseCount (assumed unique from previous merge)")

    // Step 3: Collect update IDs using broadcast filter (single base scan)
    // Collects delta IDs found in base - these are "updates". Safe to collect
    // because result set is bounded by delta size (hundreds of thousands).
    println("Finding update IDs (base records that exist in delta)...")
    val isDeltaId = udf((id: String) => deltaIdsBroadcast.value.contains(id))
    val updateIds: Set[String] = spark.read.format("avro").load(basePath)
      .select("id")
      .filter(isDeltaId(col("id")))
      .collect()
      .map(_.getString(0))
      .toSet
    val mergeUpdateCount = updateIds.size.toLong
    println(s"Records to update: $mergeUpdateCount")

    val newRecordsCount = deltaDedupCount - mergeUpdateCount
    println(s"New records to insert: $newRecordsCount")

    // Step 4: Merge and write directly to output Avro (single base scan + write)
    // This is the key operation: read base, filter out delta IDs, union with
    // delta, and write directly to output - all in a single streaming pass.
    println("Merging and writing output...")
    new File(outputPath).mkdirs()
    spark.read.format("avro").load(basePath)
      .filter(!isDeltaId(col("id")))      // Keep base records not in delta
      .union(deltaHarvestDedupDf)          // Add all delta records (updates + new)
      .write
      .mode(SaveMode.Overwrite)
      .format("avro")
      .save(outputPath)

    // Count the written output (re-read from output Avro)
    val mergeTotalCount = spark.read.format("avro").load(outputPath).count()
    println(s"Merged total: $mergeTotalCount (expected: ${newRecordsCount + baseDedupCount})")

    // Step 5: Process deletes
    println("Processing deletes...")
    val deletesDf = getIdsToDeleteDf(deletesPath, spark)
    val deleteInFileCount = deletesDf.count()
    println(s"Delete IDs in file: $deleteInFileCount")

    val deleteIds: Set[String] = if (deleteInFileCount > 0) {
      deletesDf.select("id").collect().map(_.getString(0)).toSet
    } else {
      Set.empty[String]
    }
    val deleteIdsBroadcast = spark.sparkContext.broadcast(deleteIds)
    val isDeleteId = udf((id: String) => deleteIdsBroadcast.value.contains(id))

    // Collect valid delete IDs from the merged output (small scan)
    val validDeleteIds: Set[String] = if (deleteIds.nonEmpty) {
      spark.read.format("avro").load(outputPath)
        .select("id")
        .filter(isDeleteId(col("id")))
        .collect()
        .map(_.getString(0))
        .toSet
    } else Set.empty[String]
    val validDeletesCount = validDeleteIds.size.toLong
    val invalidDeletesCount = deleteInFileCount - validDeletesCount

    // Apply deletes if needed - rewrite output with deletes filtered out
    val mergedWithDeletesCount = if (validDeleteIds.nonEmpty) {
      val deleteTmpPath = outputPath + "__tmp_pre_delete"
      // Write filtered output to temp location
      spark.read.format("avro").load(outputPath)
        .filter(!isDeleteId(col("id")))
        .write
        .mode(SaveMode.Overwrite)
        .format("avro")
        .save(deleteTmpPath)
      // Read back, count, then overwrite original output
      val filteredDf = spark.read.format("avro").load(deleteTmpPath)
      val count = filteredDf.count()
      filteredDf.write.mode(SaveMode.Overwrite).format("avro").save(outputPath)
      // Clean up temp directory
      deleteDir(new File(deleteTmpPath))
      count
    } else {
      mergeTotalCount
    }
    println(s"After deletes: $mergedWithDeletesCount (deleted: ${mergeTotalCount - mergedWithDeletesCount}, valid: $validDeletesCount, invalid: $invalidDeletesCount)")

    // Step 6: Build operations log from collected ID sets
    // All sets are small (bounded by delta + delete sizes), so construct
    // DataFrames from Scala sequences - no additional Avro scans needed.
    println("Building operations log...")
    val emptyOps = Seq.empty[(String, String)].toDF("id", "operation")

    // Insert = delta IDs not in base (not in updateIds set)
    val insertIds = deltaIds -- updateIds
    val insertOps = if (insertIds.nonEmpty)
      insertIds.toSeq.map(id => (id, "insert")).toDF("id", "operation")
    else emptyOps

    // Update = delta IDs found in base
    val updateOps = if (updateIds.nonEmpty)
      updateIds.toSeq.map(id => (id, "update")).toDF("id", "operation")
    else emptyOps

    // Delete = IDs from delete file that existed in merged output
    val deleteOps = if (validDeleteIds.nonEmpty)
      validDeleteIds.toSeq.map(id => (id, "delete")).toDF("id", "operation")
    else emptyOps

    // Invalid delete = IDs from delete file not found in merged output
    val invalidDeleteIds = deleteIds -- validDeleteIds
    val invalidDeleteOps = if (invalidDeleteIds.nonEmpty)
      invalidDeleteIds.toSeq.map(id => (id, "invalid delete")).toDF("id", "operation")
    else emptyOps

    val opsDf: DataFrame = insertOps.union(updateOps).union(deleteOps).union(invalidDeleteOps)

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

    // Clean up temp delta dedup directory (recursive)
    deleteDir(new File(deltaTmpPath))

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

  /** Recursively delete a directory and all its contents */
  private def deleteDir(dir: File): Unit = {
    try {
      if (dir.exists()) {
        Option(dir.listFiles()).foreach(_.foreach { f =>
          if (f.isDirectory) deleteDir(f) else f.delete()
        })
        dir.delete()
      }
    } catch { case _: Exception => /* ignore cleanup errors */ }
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
