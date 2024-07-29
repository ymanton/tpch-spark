package main.scala

import org.apache.spark.sql.DataFrame

trait TpchSchemaProvider {
  val tables = "customer" :: "lineitem" :: "nation" :: "region" :: "orders" :: "part" :: "partsupp" :: "supplier" :: Nil
  val customer: DataFrame
  val lineitem: DataFrame
  val nation: DataFrame
  val region: DataFrame
  val order: DataFrame
  val part: DataFrame
  val partsupp: DataFrame
  val supplier: DataFrame
}
