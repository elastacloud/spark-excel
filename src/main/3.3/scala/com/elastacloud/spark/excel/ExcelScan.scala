/*
 * Copyright 2021 Elastacloud Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.elastacloud.spark.excel

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.TextBasedFileScan
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

import scala.collection.JavaConverters.mapAsScalaMapConverter

private[excel] case class ExcelScan(
                                     sparkSession: SparkSession,
                                     fileIndex: PartitioningAwareFileIndex,
                                     dataSchema: StructType,
                                     readDataSchema: StructType,
                                     readPartitionSchema: StructType,
                                     options: CaseInsensitiveStringMap,
                                     partitionFilters: Seq[Expression] = Seq.empty,
                                     dataFilters: Seq[Expression] = Seq.empty
                                   ) extends TextBasedFileScan(sparkSession, options) {
  private val optionsAsScala = options.asScala.toMap
  private val excelOptions = ExcelParserOptions.from(options)

  override def isSplitable(path: Path): Boolean = false

  override def getFileUnSplittableReason(path: Path): String = {
    s"Excel file ${path.toString} must be read in full"
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(optionsAsScala)
    val broadcastConf = sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    ExcelPartitionReaderFactory(sparkSession.sessionState.conf, broadcastConf, dataSchema, readDataSchema, readPartitionSchema, excelOptions)
  }
}
