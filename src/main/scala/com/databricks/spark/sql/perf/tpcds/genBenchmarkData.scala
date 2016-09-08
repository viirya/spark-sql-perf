/*
 * Copyright 2016 Liang-Chi Hsieh.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.databricks.spark.sql.perf.tpcds

import java.net.InetAddress

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkContext, SparkConf}

import scala.util.Try

case class RunConfig(
    scaleFactor: Int = 1,
    dsdgenLocation: String = "",
    dataLocation: String = "",
    format: String = "parquet",
    databaseName: String = "default",
    partitionTables: Boolean = false,
    overwrite: Boolean = true,
    useDoubleForDecimal: Boolean = true,
    clusterByPartitionColumns: Boolean = true,
    filterOutNullPartitionValues: Boolean = true,
    externalTable: Boolean = true)

/**
 * Generates the data required to run TPC-DS benchmark using dsdgen tools.
 */
object genBenchmarkData {
  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[RunConfig]("spark-sql-perf-tpcds") {
      head("spark-sql-perf-tpcds", "0.2.0")
      opt[String]('d', "dsdgenLocation")
        .action { (x, c) => c.copy(dsdgenLocation = x) }
        .text("dsdgen tools location")
        .required()
      opt[String]('l', "dataLocation")
        .action((x, c) => c.copy(dataLocation = x))
        .text("data location")
        .required()
      opt[Int]('s', "scaleFactor")
        .action((x, c) => c.copy(scaleFactor = x))
        .text("the scale of generated data")
      help("help")
        .text("prints this usage text")
    }

    parser.parse(args, RunConfig()) match {
      case Some(config) =>
        run(config)
      case None =>
        System.exit(1)
    }
  }

  def run(config: RunConfig): Unit = {
    val conf = new SparkConf()
        .setMaster("local[*]")
        .setAppName(getClass.getName)

    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
 
    val tables = new Tables(sqlContext, config.dsdgenLocation, config.scaleFactor)

    tables.genData(config.dataLocation, config.format, config.overwrite, config.partitionTables,
      config.useDoubleForDecimal, config.clusterByPartitionColumns,
      config.filterOutNullPartitionValues)

    if (config.externalTable) {
      tables.createExternalTables(config.dataLocation, config.format, config.databaseName,
      config.overwrite)
    } else {
      tables.createTemporaryTables(config.dataLocation, config.format)
    }
  }
}
