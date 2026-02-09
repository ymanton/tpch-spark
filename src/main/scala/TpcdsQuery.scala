package main.scala

import java.io.{BufferedWriter, File, FileWriter}
import scala.collection.mutable.ListBuffer
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Parent class for TPC-DS queries.
 *
 * Defines schemas for tables and reads data files into these tables.
 */
abstract class TpcdsQuery {

  // get the name of the class excluding dollar signs and package
  private def escapeClassName(className: String): String = {
    className.split("\\.").last.replaceAll("\\$", "")
  }

  def getName(): String = escapeClassName(this.getClass.getName)

  /**
   * Implemented in children classes and holds the actual query
   */
  def execute(spark: SparkSession, tpcdsSchemaProvider: TpcdsSchemaProvider): DataFrame
}

object TpcdsQuery {

  def outputDF(df: DataFrame, outputDir: String, className: String): Unit = {
    if (outputDir == null || outputDir == "")
      df.collect().foreach(println)
    else {
      //df.write.mode("overwrite").json(outputDir + "/" + className + ".out") // json to avoid alias
      df.write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").save(outputDir + "/" + className)
    }
  }

  def executeQueries(spark: SparkSession, schemaProvider: TpcdsSchemaProvider, queries: Seq[Int], sqlDir: String, queryOutputDir: String): ListBuffer[(String, Float)] = {
    val executionTimes = new ListBuffer[(String, Float)]
    for (queryNo <- queries) {
      val query_name = f"query${queryNo}%d.sql"

      val log = LogManager.getRootLogger

      try {
        val query = new TpcdsSqlQuery(sqlDir + query_name)
        spark.sparkContext.setJobDescription(query.getName())
        println(f"Starting ${query.getName()}%s")

        val startTime = System.nanoTime()
        val queryOutput = query.execute(spark, schemaProvider)
        outputDF(queryOutput, queryOutputDir, query.getName())
        val endTime = System.nanoTime()

        val elapsed = (endTime - startTime) / 1000000000.0f // to seconds
        executionTimes += new Tuple2(query.getName(), elapsed)
        println(f"Finished ${query.getName()}%s in ${elapsed}%1.8f s")
      }
      catch {
        case e: Exception => log.warn(f"Failed to execute query ${query_name}: ${e}")
      }
    }

    return executionTimes
  }

  def main(args: Array[String]): Unit = {
    // TPC-DS has 99 queries (query1 to query99)
    val queries: Seq[Int] = if (args.length > 0) args.map(q => Integer.parseInt(q)) else Range.inclusive(1,99)

    // get paths from env variables else use default
    val cwd = System.getProperty("user.dir")
    val inputDataDir = sys.env.getOrElse("TPCDS_INPUT_DATA_DIR", "file://" + cwd + "/tpcds-data")
    val queryOutputDir = sys.env.getOrElse("TPCDS_QUERY_OUTPUT_DIR", inputDataDir + "/output")
    val executionTimesPath = sys.env.getOrElse("TPCDS_EXECUTION_TIMES", cwd + "/tpcds_execution_times.txt")
    val sqlDir = sys.env.getOrElse("TPCDS_QUERY_SQL_DIR", cwd + "/src/sql/tpcds") + "/"

    val spark = SparkSession
      .builder
      .appName("TPC-DS Spark")
      .getOrCreate()
    val dataSuffixEnvVar = "TPCDS_INPUT_DATA_SUFFIX"
    val schemaProvider = sys.env.getOrElse("TPCDS_INPUT_DATA_FORMAT", "text") match {
      case "parquet" => new TpcdsParquetSchemaProvider(spark, inputDataDir, sys.env.getOrElse(dataSuffixEnvVar, ".parquet"))
      case "text" => new TpcdsTextSchemaProvider(spark, inputDataDir, sys.env.getOrElse(dataSuffixEnvVar, ".dat"))
    }

    // execute queries
    val executionTimes = executeQueries(spark, schemaProvider, queries, sqlDir, queryOutputDir)
    spark.close()

    // write execution times to file
    if (executionTimes.length > 0) {
      val outfile = new File(executionTimesPath)
      val bw = new BufferedWriter(new FileWriter(outfile, true))

      bw.write(f"Query\tTime (seconds)\n")
      executionTimes.foreach {
        case (key, value) => bw.write(f"${key}%s\t${value}%1.8f\n")
      }
      bw.close()

      println(f"Execution times written in ${outfile}.")
    }

    println("Execution complete.")
  }
}
