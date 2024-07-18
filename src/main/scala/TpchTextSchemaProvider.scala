package main.scala

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataTypes, IntegerType, CharType, VarcharType, DateType, StructField, StructType}

class TpchTextSchemaProvider(spark: SparkSession, inputDir: String) extends TpchSchemaProvider {
  // TPC-H table schemas
  private val dfSchemaMap = Map(
    "customer" -> StructType(
      StructField("c_custkey", IntegerType) :: // primary key
        StructField("c_name", VarcharType(25)) ::
        StructField("c_address", VarcharType(40)) ::
        StructField("c_nationkey", IntegerType) ::
        StructField("c_phone", CharType(15)) ::
        StructField("c_acctbal", DataTypes.createDecimalType(15, 2)) ::
        StructField("c_mktsegment", CharType(10)) ::
        StructField("c_comment", VarcharType(117)) ::
        StructField("c_null", VarcharType(10)) :: Nil),
    "lineitem" -> StructType(
      StructField("l_orderkey", IntegerType) :: // primary key
        StructField("l_partkey", IntegerType) ::
        StructField("l_suppkey", IntegerType) ::
        StructField("l_linenumber", IntegerType) :: // primary key
        StructField("l_quantity", DataTypes.createDecimalType(15, 2)) ::
        StructField("l_extendedprice", DataTypes.createDecimalType(15, 2)) ::
        StructField("l_discount", DataTypes.createDecimalType(15, 2)) ::
        StructField("l_tax", DataTypes.createDecimalType(15, 2)) ::
        StructField("l_returnflag", CharType(1)) ::
        StructField("l_linestatus", CharType(1)) ::
        StructField("l_shipdate", DateType) ::
        StructField("l_commitdate", DateType) ::
        StructField("l_receiptdate", DateType) ::
        StructField("l_shipinstruct", CharType(25)) ::
        StructField("l_shipmode", CharType(10)) ::
        StructField("l_comment", VarcharType(44)) ::
        StructField("l_null", VarcharType(10)) :: Nil),
    "nation" -> StructType(
      StructField("n_nationkey", IntegerType) :: // primary key
        StructField("n_name", CharType(25)) ::
        StructField("n_regionkey", IntegerType) ::
        StructField("n_comment", VarcharType(152)) ::
        StructField("n_null", VarcharType(10)) :: Nil),
    "orders" -> StructType(
      StructField("o_orderkey", IntegerType) :: // primary key
        StructField("o_custkey", IntegerType) ::
        StructField("o_orderstatus", CharType(1)) ::
        StructField("o_totalprice", DataTypes.createDecimalType(15, 2)) ::
        StructField("o_orderdate", CharType(15)) ::
        StructField("o_orderpriority", CharType(15)) ::
        StructField("o_clerk", CharType(15)) ::
        StructField("o_shippriority", IntegerType) ::
        StructField("o_comment", VarcharType(79)) ::
        StructField("o_null", VarcharType(10)) :: Nil),
    "part" -> StructType(
      StructField("p_partkey", IntegerType) :: // primary key
        StructField("p_name", VarcharType(55)) ::
        StructField("p_mfgr", CharType(25)) ::
        StructField("p_brand", CharType(10)) ::
        StructField("p_type", VarcharType(25)) ::
        StructField("p_size", IntegerType) ::
        StructField("p_container", CharType(10)) ::
        StructField("p_retailprice", DataTypes.createDecimalType(15, 2)) ::
        StructField("p_comment", VarcharType(23)) ::
        StructField("p_null", VarcharType(10)) :: Nil),
    "partsupp" -> StructType(
      StructField("ps_partkey", IntegerType) :: // primary key
        StructField("ps_suppkey", IntegerType) :: // primary key
        StructField("ps_availqty", IntegerType) ::
        StructField("ps_supplycost", DataTypes.createDecimalType(15, 2)) ::
        StructField("ps_comment", VarcharType(199)) ::
        StructField("ps_null", VarcharType(10)) :: Nil),
    "region" -> StructType(
      StructField("r_regionkey", IntegerType) :: // primary key
        StructField("r_name", CharType(10)) ::
        StructField("r_comment", VarcharType(152)) ::
        StructField("r_null", VarcharType(10)) :: Nil),
    "supplier" -> StructType(
      StructField("s_suppkey", IntegerType) :: // primary key
        StructField("s_name", CharType(25)) ::
        StructField("s_address", VarcharType(40)) ::
        StructField("s_nationkey", IntegerType) ::
        StructField("s_phone", CharType(15)) ::
        StructField("s_acctbal", DataTypes.createDecimalType(15, 2)) ::
        StructField("s_comment", VarcharType(101)) ::
        StructField("s_null", VarcharType(10)) :: Nil)
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
    case (key, value) => value.createOrReplaceTempView(key)
  }
}
