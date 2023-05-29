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
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{CodecStreams, PartitionedFile}
import org.apache.spark.sql.types.StructType

import java.net.URI

private[excel] class ExcelDataIterator(options: ExcelParserOptions) extends Logging {
  def readFile(conf: Configuration, file: PartitionedFile, schema: StructType): Iterator[InternalRow] = {
    val inputStream = CodecStreams.createInputStreamWithCloseResource(conf, file.toPath)

    Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => inputStream.close()))

    val parser = new ExcelParser(inputStream, options, Some(schema), Some(schema))
    parser.getDataIterator.map(it => InternalRow.fromSeq(it))
  }
}
