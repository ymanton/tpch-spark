package main.scala

import org.apache.spark.sql.SparkSession

object ConvertToParquet {
  def convert(spark: SparkSession, schemaProvider: TpchSchemaProvider, parquetOutputDir: String, dataSuffix: String): Unit = {
    schemaProvider.customer.write.parquet(parquetOutputDir + "/customer" + dataSuffix)
    schemaProvider.lineitem.write.parquet(parquetOutputDir + "/lineitem" + dataSuffix)
    schemaProvider.nation.write.parquet(parquetOutputDir + "/nation" + dataSuffix)
    schemaProvider.order.write.parquet(parquetOutputDir + "/orders" + dataSuffix)
    schemaProvider.partsupp.write.parquet(parquetOutputDir + "/partsupp" + dataSuffix)
    schemaProvider.part.write.parquet(parquetOutputDir + "/part" + dataSuffix)
    schemaProvider.region.write.parquet(parquetOutputDir + "/region" + dataSuffix)
    schemaProvider.supplier.write.parquet(parquetOutputDir + "/supplier" + dataSuffix)
  }
  def main(args: Array[String]): Unit = {
    val cwd = System.getProperty("user.dir")
    val inputDataDir = sys.env.getOrElse("TPCH_INPUT_DATA_DIR", "file://" + cwd + "/dbgen")
    val inputDataSuffix = sys.env.getOrElse("TPCH_INPUT_DATA_SUFFIX", ".tbl")
    val outputDataSuffix = sys.env.getOrElse("TPCH_OUTPUT_DATA_SUFFIX", ".parquet")
    val parquetOutputDir = sys.env.getOrElse("TPCH_PARQUET_OUTPUT_DIR", inputDataDir)

    val spark = SparkSession
      .builder
      .appName("Convert To Parquet tool")
      .getOrCreate()
    val schemaProvider = new TpchTextSchemaProvider(spark, inputDataDir, inputDataSuffix)
    convert(spark, schemaProvider, parquetOutputDir, outputDataSuffix)
    spark.close()
  }
}

