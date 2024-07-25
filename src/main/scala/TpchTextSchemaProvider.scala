package main.scala

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataTypes, IntegerType, StringType, DateType, StructField, StructType}

class TpchTextSchemaProvider(spark: SparkSession, inputDir: String) extends TpchSchemaProvider {
  // TPC-H table schemas
  private val dfSchemaMap = Map(
    "customer" -> StructType(
      StructField("c_custkey", IntegerType) :: // primary key
        StructField("c_name", StringType) ::
        StructField("c_address", StringType) ::
        StructField("c_nationkey", IntegerType) ::
        StructField("c_phone", StringType) ::
        StructField("c_acctbal", DataTypes.createDecimalType(15, 2)) ::
        StructField("c_mktsegment", StringType) ::
        StructField("c_comment", StringType) :: Nil),
    "lineitem" -> StructType(
      StructField("l_orderkey", IntegerType) :: // primary key
        StructField("l_partkey", IntegerType) ::
        StructField("l_suppkey", IntegerType) ::
        StructField("l_linenumber", IntegerType) :: // primary key
        StructField("l_quantity", DataTypes.createDecimalType(15, 2)) ::
        StructField("l_extendedprice", DataTypes.createDecimalType(15, 2)) ::
        StructField("l_discount", DataTypes.createDecimalType(15, 2)) ::
        StructField("l_tax", DataTypes.createDecimalType(15, 2)) ::
        StructField("l_returnflag", StringType) ::
        StructField("l_linestatus", StringType) ::
        StructField("l_shipdate", DateType) ::
        StructField("l_commitdate", DateType) ::
        StructField("l_receiptdate", DateType) ::
        StructField("l_shipinstruct", StringType) ::
        StructField("l_shipmode", StringType) ::
        StructField("l_comment", StringType) :: Nil),
    "nation" -> StructType(
      StructField("n_nationkey", IntegerType) :: // primary key
        StructField("n_name", StringType) ::
        StructField("n_regionkey", IntegerType) ::
        StructField("n_comment", StringType) :: Nil),
    "orders" -> StructType(
      StructField("o_orderkey", IntegerType) :: // primary key
        StructField("o_custkey", IntegerType) ::
        StructField("o_orderstatus", StringType) ::
        StructField("o_totalprice", DataTypes.createDecimalType(15, 2)) ::
        StructField("o_orderdate", StringType) ::
        StructField("o_orderpriority", StringType) ::
        StructField("o_clerk", StringType) ::
        StructField("o_shippriority", IntegerType) ::
        StructField("o_comment", StringType) :: Nil),
    "part" -> StructType(
      StructField("p_partkey", IntegerType) :: // primary key
        StructField("p_name", StringType) ::
        StructField("p_mfgr", StringType) ::
        StructField("p_brand", StringType) ::
        StructField("p_type", StringType) ::
        StructField("p_size", IntegerType) ::
        StructField("p_container", StringType) ::
        StructField("p_retailprice", DataTypes.createDecimalType(15, 2)) ::
        StructField("p_comment", StringType) :: Nil),
    "partsupp" -> StructType(
      StructField("ps_partkey", IntegerType) :: // primary key
        StructField("ps_suppkey", IntegerType) :: // primary key
        StructField("ps_availqty", IntegerType) ::
        StructField("ps_supplycost", DataTypes.createDecimalType(15, 2)) ::
        StructField("ps_comment", StringType) :: Nil),
    "region" -> StructType(
      StructField("r_regionkey", IntegerType) :: // primary key
        StructField("r_name", StringType) ::
        StructField("r_comment", StringType) :: Nil),
    "supplier" -> StructType(
      StructField("s_suppkey", IntegerType) :: // primary key
        StructField("s_name", StringType) ::
        StructField("s_address", StringType) ::
        StructField("s_nationkey", IntegerType) ::
        StructField("s_phone", StringType) ::
        StructField("s_acctbal", DataTypes.createDecimalType(15, 2)) ::
        StructField("s_comment", StringType) :: Nil)
  )

  private val dfMap = dfSchemaMap.map {
    case (name, schema) => (name, spark.read.schema(schema).option("delimiter", "|").csv(inputDir + s"/$name.tbl*"))
  }

  // for implicits
  val customer: DataFrame = dfMap("customer")
  val lineitem: DataFrame = dfMap("lineitem")
  val nation: DataFrame = dfMap("nation")
  val region: DataFrame = dfMap("region")
  val order: DataFrame = dfMap("orders")
  val part: DataFrame = dfMap("part")
  val partsupp: DataFrame = dfMap("partsupp")
  val supplier: DataFrame = dfMap("supplier")

  dfMap.foreach {
    case (key, value) => value.show()
  }

  dfMap.foreach {
    case (key, value) => value.createOrReplaceTempView(key)
  }
}
