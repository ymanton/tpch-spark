package main.scala

import org.apache.spark.sql.{DataFrame, SparkSession}

class TpchParquetSchemaProvider(spark: SparkSession, inputDir: String) extends TpchSchemaProvider {
  import spark.implicits._

  private var dfMap = Map[String, DataFrame]()

  for (t <- tables) {
    spark.sparkContext.setJobDescription(s"$t.parquet")
    val df = spark.read.parquet(s"$inputDir/$t.parquet")
    df.createOrReplaceTempView(t)
    dfMap += (t -> df)
  }

  // for implicits
  val customer = dfMap.get("customer").get
  val lineitem = dfMap.get("lineitem").get
  val nation = dfMap.get("nation").get
  val region = dfMap.get("region").get
  val order = dfMap.get("orders").get
  val part = dfMap.get("part").get
  val partsupp = dfMap.get("partsupp").get
  val supplier = dfMap.get("supplier").get
}
