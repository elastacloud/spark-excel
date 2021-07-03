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
import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters.mapAsScalaMapConverter

private[excel] class ExcelTable(
                                 name: String,
                                 sparkSession: SparkSession,
                                 options: CaseInsensitiveStringMap,
                                 paths: Seq[String],
                                 userSpecifiedSchema: Option[StructType],
                               ) extends FileTable(sparkSession, options, paths, userSpecifiedSchema) {
  override def newScanBuilder(options: CaseInsensitiveStringMap): ExcelScanBuilder =
    ExcelScanBuilder(sparkSession, fileIndex, schema, dataSchema, options)

  override def inferSchema(files: Seq[FileStatus]): Option[StructType] = {
    if (files.isEmpty) return None

    val sampleFile = files.head.getPath
    val conf = sparkSession.sessionState.newHadoopConfWithOptions(options.asScala.toMap)
    val fs = sampleFile.getFileSystem(conf)
    val inputStream = fs.open(sampleFile)

    try {
      Some(ExcelParser.schemaFromWorkbook(inputStream, ExcelParserOptions.from(options)))
    } finally {
      inputStream.close()
    }
  }

  override def formatName: String = "Excel"

  override def fallbackFileFormat: Class[_ <: FileFormat] = classOf[ExcelFileSource]

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = throw new RuntimeException("Write is not supported")

  override def name(): String = name
}
