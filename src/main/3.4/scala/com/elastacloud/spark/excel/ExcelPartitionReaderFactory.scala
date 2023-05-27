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

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.{FileSourceOptions, InternalRow}
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2.{FilePartitionReaderFactory, PartitionReaderWithPartitionValues}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

private[excel] case class ExcelPartitionReaderFactory(
                                                       sqlConf: SQLConf,
                                                       broadcastConf: Broadcast[SerializableConfiguration],
                                                       dataSchema: StructType,
                                                       readDataSchema: StructType,
                                                       partitionSchema: StructType,
                                                       excelOptions: ExcelParserOptions
                                                     ) extends FilePartitionReaderFactory {
  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
    val reader = new ExcelPartitionReader(partitionedFile, broadcastConf, excelOptions, dataSchema, readDataSchema)
    new PartitionReaderWithPartitionValues(reader, readDataSchema, partitionSchema, partitionedFile.partitionValues)
  }

  override protected def options: FileSourceOptions = excelOptions
}
