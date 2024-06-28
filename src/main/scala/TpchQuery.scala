package main.scala

import java.io.{BufferedWriter, File, FileWriter}
import scala.collection.mutable.ListBuffer
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Parent class for TPC-H queries.
 *
 * Defines schemas for tables and reads pipe ("|") separated text files into these tables.
 */
abstract class TpchQuery {

  // get the name of the class excluding dollar signs and package
  private def escapeClassName(className: String): String = {
    className.split("\\.").last.replaceAll("\\$", "")
  }

  def getName(): String = escapeClassName(this.getClass.getName)

  /**
   * Implemented in children classes and holds the actual query
   */
  def execute(spark: SparkSession, tpchSchemaProvider: TpchSchemaProvider): DataFrame
}

object TpchQuery {

  def outputDF(df: DataFrame, outputDir: String, className: String): Unit = {
    if (outputDir == null || outputDir == "")
      df.collect().foreach(println)
    else {
      //df.write.mode("overwrite").json(outputDir + "/" + className + ".out") // json to avoid alias
      df.write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").save(outputDir + "/" + className)
    }
  }

  def executeQueries(spark: SparkSession, schemaProvider: TpchSchemaProvider, queryNum: Option[Int], queryOutputDir: String): ListBuffer[(String, Float)] = {
    var queryFrom = 1;
    var queryTo = 22;
    queryNum match {
      case Some(n) => {
        queryFrom = n
        queryTo = n
      }
      case None => {}
    }

    val executionTimes = new ListBuffer[(String, Float)]
    for (queryNo <- queryFrom to queryTo) {
      val startTime = System.nanoTime()
      val query_name = f"main.scala.Q${queryNo}%02d"

      val log = LogManager.getRootLogger

      try {
        val query = Class.forName(query_name).newInstance.asInstanceOf[TpchQuery]
        val queryOutput = query.execute(spark, schemaProvider)
        outputDF(queryOutput, queryOutputDir, query.getName())

        val endTime = System.nanoTime()
        val elapsed = (endTime - startTime) / 1000000000.0f // to seconds
        executionTimes += new Tuple2(query.getName(), elapsed)
      }
      catch {
        case e: Exception => log.warn(f"Failed to execute query ${query_name}: ${e}")
      }
    }

    return executionTimes
  }

  def main(args: Array[String]): Unit = {
    // parse command line arguments: expecting _at most_ 1 argument denoting which query to run
    // if no query is given, all queries 1..22 are run.
    if (args.length > 1)
      println("Expected at most 1 argument: query to run. No arguments = run all 22 queries.")
    val queryNum = if (args.length == 1) {
      try {
        Some(Integer.parseInt(args(0).trim))
      } catch {
        case e: Exception => None
      }
    } else
      None

    // get paths from env variables else use default
    val cwd = System.getProperty("user.dir")
    val inputDataDir = sys.env.getOrElse("TPCH_INPUT_DATA_DIR", "file://" + cwd + "/dbgen")
    val queryOutputDir = sys.env.getOrElse("TPCH_QUERY_OUTPUT_DIR", inputDataDir + "/output")
    val executionTimesPath = sys.env.getOrElse("TPCH_EXECUTION_TIMES", cwd + "/tpch_execution_times.txt")

    val spark = SparkSession
      .builder
      .appName("TPC-H v3.0.0 Spark")
      .getOrCreate()
    val schemaProvider = sys.env.getOrElse("TPCH_INPUT_DATA_FORMAT", "text") match {
      case "parquet" => new TpchParquetSchemaProvider(spark, inputDataDir)
      case "text" => new TpchSchemaProvider(spark, inputDataDir)
    }

    // execute queries
    val executionTimes = executeQueries(spark, schemaProvider, queryNum, queryOutputDir)
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
