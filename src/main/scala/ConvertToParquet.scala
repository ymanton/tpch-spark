package main.scala

import org.apache.spark.sql.SparkSession

object ConvertToParquet {
  def executeQueries(spark: SparkSession, schemaProvider: TpchSchemaProvider, parquetOutputDir: String): Unit = {
    schemaProvider.customer.write.parquet(parquetOutputDir + "/customer.parquet")
    schemaProvider.lineitem.write.parquet(parquetOutputDir + "/lineitem.parquet")
    schemaProvider.nation.write.parquet(parquetOutputDir + "/nation.parquet")
    schemaProvider.orders.write.parquet(parquetOutputDir + "/orders.parquet")
    schemaProvider.partsupp.write.parquet(parquetOutputDir + "/partsupp.parquet")
    schemaProvider.part.write.parquet(parquetOutputDir + "/part.parquet")
    schemaProvider.region.write.parquet(parquetOutputDir + "/region.parquet")
    schemaProvider.supplier.write.parquet(parquetOutputDir + "/supplier.parquet")
  }
  def main(args: Array[String]): Unit = {
    val cwd = System.getProperty("user.dir")
    val inputDataDir = sys.env.getOrElse("TPCH_INPUT_DATA_DIR", "file://" + cwd + "/dbgen")
    val parquetOutputDir = sys.env.getOrElse("TPCH_PARQUET_OUTPUT_DIR", inputDataDir)

    val spark = SparkSession
      .builder
      .appName("Convert To Parquet tool")
      .getOrCreate()
    val schemaProvider = new TpchSchemaProvider(spark, inputDataDir)
    convert(spark, schemaProvider, parquetOutputDir)
    spark.close()
  }
}

