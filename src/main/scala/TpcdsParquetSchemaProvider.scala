package main.scala

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * TPC-DS schema provider for Parquet data files.
 * 
 * This provider reads TPC-DS tables from Parquet format. The schema is inferred
 * from the Parquet files, so no explicit schema definition is needed.
 */
class TpcdsParquetSchemaProvider(spark: SparkSession, inputDir: String, dataSuffix: String) extends TpcdsSchemaProvider {
  import spark.implicits._

  private var dfMap = Map[String, DataFrame]()

  // Load all tables from Parquet files
  for (t <- tables) {
    spark.sparkContext.setJobDescription(s"$t$dataSuffix")
    val df = spark.read.parquet(s"$inputDir/$t$dataSuffix")
    df.createOrReplaceTempView(t)
    dfMap += (t -> df)
  }

  // DataFrame properties for each table
  val call_center = dfMap.get("call_center").get
  val catalog_page = dfMap.get("catalog_page").get
  val catalog_returns = dfMap.get("catalog_returns").get
  val catalog_sales = dfMap.get("catalog_sales").get
  val customer = dfMap.get("customer").get
  val customer_address = dfMap.get("customer_address").get
  val customer_demographics = dfMap.get("customer_demographics").get
  val date_dim = dfMap.get("date_dim").get
  val household_demographics = dfMap.get("household_demographics").get
  val income_band = dfMap.get("income_band").get
  val inventory = dfMap.get("inventory").get
  val item = dfMap.get("item").get
  val promotion = dfMap.get("promotion").get
  val reason = dfMap.get("reason").get
  val ship_mode = dfMap.get("ship_mode").get
  val store = dfMap.get("store").get
  val store_returns = dfMap.get("store_returns").get
  val store_sales = dfMap.get("store_sales").get
  val time_dim = dfMap.get("time_dim").get
  val warehouse = dfMap.get("warehouse").get
  val web_page = dfMap.get("web_page").get
  val web_returns = dfMap.get("web_returns").get
  val web_sales = dfMap.get("web_sales").get
  val web_site = dfMap.get("web_site").get
}
