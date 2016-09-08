/*
 * Copyright 2015 Liang-Chi Hsieh
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

case class TPCDSConfig(
    dataLocation: String = "",
    format: String = "parquet",
    databaseName: String = "default",
    externalTable: Boolean = true,
    overwrite: Boolean = true,
    queries: Seq[String] = Seq.empty[String])

/**
 * Runs TPC-DS benchmark.
 */
object runTPCDSBenchmark {
  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[TPCDSConfig]("spark-sql-perf-tpcds") {
      head("spark-sql-perf-tpcds", "0.2.0")
      opt[String]('l', "dataLocation")
        .action((x, c) => c.copy(dataLocation = x))
        .text("data location")
        .required()
      opt[Boolean]('e', "externalTable")
        .action((x, c) => c.copy(externalTable = x))
        .text("Use metastore table")
      opt[String]('q', "queries")
        .action((x, c) => c.copy(queries = x.split(",").map(_.trim)))
        .text("Queries to run")
        
      help("help")
        .text("prints this usage text")
    }

    parser.parse(args, TPCDSConfig()) match {
      case Some(config) =>
        run(config)
      case None =>
        System.exit(1)
    }
  }

  def run(config: TPCDSConfig): Unit = {
    val conf = new SparkConf()
        .setMaster("local[*]")
        .setAppName(getClass.getName)

    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
 
    val tables = new Tables(sqlContext, "", 1)

    if (config.externalTable) {
      tables.createExternalTables(config.dataLocation, config.format, config.databaseName,
      config.overwrite)
    } else {
      tables.createTemporaryTables(config.dataLocation, config.format)
    }

    val tpcds = new TPCDS(sqlContext = sqlContext)
    val runQueries = if (config.queries.isEmpty) {
      tpcds.all
    } else {
      tpcds.all.filter { q =>
        config.queries.contains(q.name)
      }
    }
    tpcds.run(runQueries)
  }
}
