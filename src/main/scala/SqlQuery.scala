package main.scala

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import java.nio.file.Files
import java.nio.file.Paths

class SqlQuery(val sqlFilePath: String) extends TpchQuery {

  val sqlPath = Paths.get(sqlFilePath)
  val queryName = sqlPath.getFileName().toString()
  val sqlText = new String(Files.readAllBytes(Paths.get(sqlFilePath)))

  override def execute(spark: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {
    import spark.implicits._
    import schemaProvider._

    spark.sql(sqlText)
  }

  override def getName(): String = queryName
}

