package main.scala

import org.apache.spark.sql.SparkSession

class TpchParquetSchemaProvider(spark: SparkSession, inputDir: String) extends TpchSchemaProvider(spark, inputDir) {
  import spark.implicits._

  override val dfMap = Map(
    "customer" -> spark.read.parquet(inputDir + "/customer.parquet"),

    "lineitem" -> spark.read.parquet(inputDir + "/lineitem.parquet"),

    "nation" -> spark.read.parquet(inputDir + "/nation.parquet"),

    "region" -> spark.read.parquet(inputDir + "/region.parquet"),

    "order" -> spark.read.parquet(inputDir + "/orders.parquet"),

    "part" -> spark.read.parquet(inputDir + "/part.parquet"),

    "partsupp" -> spark.read.parquet(inputDir + "/partsupp.parquet"),

    "supplier" -> spark.read.parquet(inputDir + "/supplier.parquet")
  )

  // for implicits
  override val customer = dfMap.get("customer").get
  override val lineitem = dfMap.get("lineitem").get
  override val nation = dfMap.get("nation").get
  override val region = dfMap.get("region").get
  override val order = dfMap.get("order").get
  override val part = dfMap.get("part").get
  override val partsupp = dfMap.get("partsupp").get
  override val supplier = dfMap.get("supplier").get

  dfMap.foreach {
    case (key, value) => value.createOrReplaceTempView(key)
  }
}
