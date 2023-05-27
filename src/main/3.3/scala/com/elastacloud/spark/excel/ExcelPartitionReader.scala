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

import com.elastacloud.spark.excel.parser.ExcelParser
import org.apache.hadoop.fs.Path
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.{CodecStreams, PartitionedFile}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

import java.net.URI

private[excel] class ExcelPartitionReader(
                                           path: PartitionedFile,
                                           broadcastConf: Broadcast[SerializableConfiguration],
                                           excelOptions: ExcelParserOptions,
                                           schema: StructType,
                                           readSchema: StructType
                                         ) extends PartitionReader[InternalRow] {
  private val filePath = new Path(new URI(path.filePath))
  private val inputStream = CodecStreams.createInputStreamWithCloseResource(broadcastConf.value.value, filePath)
  private val excelParser = new ExcelParser(inputStream, excelOptions, Some(schema), Some(readSchema))
  private val dataIterator = excelParser.getDataIterator

  override def next(): Boolean = dataIterator.hasNext

  override def get(): InternalRow = {
    val rowData = dataIterator.next()
    InternalRow.fromSeq(rowData)
  }

  override def close(): Unit = {
    excelParser.close()
    inputStream.close()
  }
}
