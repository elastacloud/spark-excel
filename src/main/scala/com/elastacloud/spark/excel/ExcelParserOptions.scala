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

import com.elastacloud.spark.excel.ExcelParserOptions.checkInvalidOptions
import org.apache.commons.codec.language.DoubleMetaphone
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.FileSourceOptions
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.convert.ImplicitConversions.`map AsScala`
import scala.collection.mutable.ArrayBuffer

/**
 * Defines the options used by the Excel parser
 * @param parameters case insensitive map of parameters to use
 */
private[excel] class ExcelParserOptions(
  @transient val parameters: CaseInsensitiveMap[String])
  extends FileSourceOptions(parameters) with Logging {

  /**
   * Defines the options used by the Excel parser
   * @param parameters map of parameters to use
   */
  def this(parameters: Map[String, String]) = {
    this(CaseInsensitiveMap(parameters))
  }

  /**
   * Defines the options used by the Excel parser, using the default options
   */
  def this() = {
    this(Map[String, String]())
  }

  checkInvalidOptions(parameters.keySet) match {
    case Some(errors) => throw new ExcelParserOptionsException(s"Unable to parse options:\n${errors.mkString("\n")}")
    case _ =>
  }

  val workbookPassword: Option[String] = parameters.get("workbookPassword")
  val sheetNamePattern: String = parameters.getOrElse("sheetNamePattern", "")
  val cellAddress: String = parameters.getOrElse("cellAddress", "A1")
  val headerRowCount: Int = parameters.getOrElse("headerRowCount", "1").toInt
  val maxRowCount: Int = parameters.getOrElse("maxRowCount", "1000").toInt
  val includeSheetName: Boolean = parameters.getOrElse("includeSheetName", "false").toBoolean
  val nulLValue: Option[String] = parameters.get("nullValue")
  val thresholdBytesForTempFiles: Int = parameters.getOrElse("thresholdBytesForTempFiles", parameters.getOrElse("maxBytesForTempFiles", "100000000")).toInt
  val evaluateFormulae: Boolean = parameters.getOrElse("evaluateFormulae", "true").toBoolean

  val schemaMatchColumnName: String = parameters.getOrElse("schemaMatchColumnName", null)
  if (schemaMatchColumnName != null && schemaMatchColumnName.trim.isEmpty) {
    throw new ExcelParserOptionsException("The 'schemaMatchColumnName' option must contain a value if provided")
  }

}

/**
 * Provides object methods for the Excel parser options
 */
private[excel] object ExcelParserOptions {

  private val encoder = new DoubleMetaphone()

  private val mappings = Map[String, String](
    encoder.encode("workbookPassword") -> "workbookPassword",
    encoder.encode("sheetNamePattern") -> "sheetNamePattern",
    encoder.encode("cellAddress") -> "cellAddress",
    encoder.encode("headerRowCount") -> "headerRowCount",
    encoder.encode("maxRowCount") -> "maxRowCount",
    encoder.encode("includeSheetName") -> "includeSheetName",
    encoder.encode("nullValue") -> "nullValue",
    encoder.encode("maxBytesForTempFiles") -> "maxBytesForTempFiles",
    encoder.encode("thresholdBytesForTempFiles") -> "thresholdBytesForTempFiles",
    encoder.encode("schemaMatchColumnName") -> "schemaMatchColumnName",
    encoder.encode("evaluateFormulae") -> "evaluateFormulae"
  )

  /**
   * Checks the provided option keys for any naming issues
   * @param keys the set of keys provided by the user
   * @return A collection of error messages, or None
   */
  private def checkInvalidOptions(keys: Set[String]): Option[Seq[String]] = {
    val buffer = new ArrayBuffer[String]()

    keys.foreach(key => {
      val encodedKey = encoder.encode(key)
      if (!mappings.values.exists(_.compareToIgnoreCase(key) == 0) && mappings.contains(encodedKey)) {
        buffer.append(s"Invalid option '${key.toLowerCase}', did you mean '${mappings(encodedKey)}'?")
      }
    })

    if (buffer.isEmpty) None else Some(buffer)
  }

  /**
   * Create a new set of parser options from a [[CaseInsensitiveStringMap]]
   * @param options a map of options to create from
   * @return a new [[ExcelParserOptions]] instance
   */
  def from(options: CaseInsensitiveStringMap): ExcelParserOptions = {
    new ExcelParserOptions(options.toMap)
  }

  /**
   * Create a new set of parser options from a [[Map]]
   *
   * @param options a map of options to create from
   * @return a new [[ExcelParserOptions]] instance
   */
  def from(options: Map[String, String]): ExcelParserOptions = {
    new ExcelParserOptions(options)
  }

}
