package main.scala

import org.apache.spark.sql._

// TPC-H table schemas
case class Customer(
  c_custkey: Int,
  c_name: String,
  c_address: String,
  c_nationkey: Int,
  c_phone: String,
  c_acctbal: Double,
  c_mktsegment: String,
  c_comment: String)

case class Lineitem(
  l_orderkey: Int,
  l_partkey: Int,
  l_suppkey: Int,
  l_linenumber: Int,
  l_quantity: Double,
  l_extendedprice: Double,
  l_discount: Double,
  l_tax: Double,
  l_returnflag: String,
  l_linestatus: String,
  l_shipdate: String,
  l_commitdate: String,
  l_receiptdate: String,
  l_shipinstruct: String,
  l_shipmode: String,
  l_comment: String)

case class Nation(
  n_nationkey: Int,
  n_name: String,
  n_regionkey: Int,
  n_comment: String)

case class Order(
  o_orderkey: Int,
  o_custkey: Int,
  o_orderstatus: String,
  o_totalprice: Double,
  o_orderdate: String,
  o_orderpriority: String,
  o_clerk: String,
  o_shippriority: Int,
  o_comment: String)

case class Part(
  p_partkey: Int,
  p_name: String,
  p_mfgr: String,
  p_brand: String,
  p_type: String,
  p_size: Int,
  p_container: String,
  p_retailprice: Double,
  p_comment: String)

case class Partsupp(
  ps_partkey: Int,
  ps_suppkey: Int,
  ps_availqty: Int,
  ps_supplycost: Double,
  ps_comment: String)

case class Region(
  r_regionkey: Int,
  r_name: String,
  r_comment: String)

case class Supplier(
  s_suppkey: Int,
  s_name: String,
  s_address: String,
  s_nationkey: Int,
  s_phone: String,
  s_acctbal: Double,
  s_comment: String)


class TpchSchemaProvider(spark: SparkSession, inputDir: String, initOutputDir: String, caches: Seq[String], sql: Boolean) {
  import spark.implicits._

  val outputDir = initOutputDir
  val dfMap = Map(
    "customer" -> spark.sparkContext.textFile(inputDir + "/customer.tbl").map(_.split('|')).map(p =>
      Customer(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim.toInt, p(4).trim, p(5).trim.toDouble, p(6).trim, p(7).trim)).toDF(),
    "lineitem" -> spark.sparkContext.textFile(inputDir + "/lineitem.tbl").map(_.split('|')).map(p =>
      Lineitem(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toInt, p(4).trim.toDouble, p(5).trim.toDouble, p(6).trim.toDouble
        , p(7).trim.toDouble, p(8).trim, p(9).trim, p(10).trim, p(11).trim, p(12).trim, p(13).trim, p(14).trim, p(15).trim)).toDF(),
    "nation" -> spark.sparkContext.textFile(inputDir + "/nation.tbl").map(_.split('|')).map(p =>
      Nation(p(0).trim.toInt, p(1).trim, p(2).trim.toInt, p(3).trim)).toDF(),
    "region" -> spark.sparkContext.textFile(inputDir + "/region.tbl").map(_.split('|')).map(p =>
      Region(p(0).trim.toInt, p(1).trim, p(1).trim)).toDF(),
    "order" -> spark.sparkContext.textFile(inputDir + "/orders.tbl").map(_.split('|')).map(p =>
      Order(p(0).trim.toInt, p(1).trim.toInt, p(2).trim, p(3).trim.toDouble, p(4).trim, p(5).trim, p(6).trim
        , p(7).trim.toInt, p(8).trim)).toDF(),
    "part" -> spark.sparkContext.textFile(inputDir + "/part.tbl").map(_.split('|')).map(p =>
      Part(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim, p(4).trim, p(5).trim.toInt, p(6).trim
        , p(7).trim.toDouble, p(8).trim)).toDF(),
    "partsupp" -> spark.sparkContext.textFile(inputDir + "/partsupp.tbl").map(_.split('|')).map(p =>
      Partsupp(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toDouble, p(4).trim)).toDF(),
    "supplier" -> spark.sparkContext.textFile(inputDir + "/supplier.tbl").map(_.split('|')).map(p =>
      Supplier(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim.toInt, p(4).trim, p(5).trim.toDouble, p(6).trim)).toDF())

  // for implicits
  val customer = dfMap.get("customer").get
  val lineitem = dfMap.get("lineimte").get
  val nation = dfMap.get("nation").get
  val region = dfMap.get("region").get
  val order = dfMap.get("order").get
  val part = dfMap.get("part").get
  val partsupp = dfMap.get("partsupp").get
  val supplier = dfMap.get("supplier").get

  if (caches.contains("all")) {
    dfMap.values.foreach(_.cache().count())
  } else {
    caches.foreach(_ => dfMap.get(_).get.cache().count())
  }

  dfMap.foreach {
    case (key, value) => value.createOrReplaceTempView(key)
  }

  def close() = {
    dfMap.values.foreach(_.unpersist())
  }
}