package main.scala

import org.apache.spark.sql.DataFrame

/**
 * Schema provider trait for TPC-DS benchmark tables.
 * 
 * TPC-DS has 24 tables. The table list and DataFrame properties need to be
 * implemented by concrete classes (TpcdsTextSchemaProvider, TpcdsParquetSchemaProvider).
 */
trait TpcdsSchemaProvider {
  // TPC-DS table names - 24 tables total
  val tables = "call_center" :: "catalog_page" :: "catalog_returns" :: "catalog_sales" :: 
               "customer" :: "customer_address" :: "customer_demographics" :: "date_dim" :: 
               "household_demographics" :: "income_band" :: "inventory" :: "item" :: 
               "promotion" :: "reason" :: "ship_mode" :: "store" :: 
               "store_returns" :: "store_sales" :: "time_dim" :: "warehouse" :: 
               "web_page" :: "web_returns" :: "web_sales" :: "web_site" :: Nil
  
  // DataFrame properties for each table
  val call_center: DataFrame
  val catalog_page: DataFrame
  val catalog_returns: DataFrame
  val catalog_sales: DataFrame
  val customer: DataFrame
  val customer_address: DataFrame
  val customer_demographics: DataFrame
  val date_dim: DataFrame
  val household_demographics: DataFrame
  val income_band: DataFrame
  val inventory: DataFrame
  val item: DataFrame
  val promotion: DataFrame
  val reason: DataFrame
  val ship_mode: DataFrame
  val store: DataFrame
  val store_returns: DataFrame
  val store_sales: DataFrame
  val time_dim: DataFrame
  val warehouse: DataFrame
  val web_page: DataFrame
  val web_returns: DataFrame
  val web_sales: DataFrame
  val web_site: DataFrame
}
