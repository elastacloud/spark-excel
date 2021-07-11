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

import org.apache.commons.codec.language.DoubleMetaphone
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.mutable.ArrayBuffer

/**
 * Defines the Excel parser options
 *
 * @param workbookPassword password required to open the workbook
 * @param sheetNamePattern a regex pattern to identify sheets to read data from
 * @param cellAddress      the cell address of the first cell in the data table
 * @param headerRowCount   number of rows in the header
 * @param maxRowCount      maximum number of rows to read when inferring the schema
 * @param includeSheetName identifies if the sheet name should be included in the output
 */
private[excel] case class ExcelParserOptions(workbookPassword: Option[String] = None,
                                             sheetNamePattern: String = "",
                                             cellAddress: String = "A1",
                                             headerRowCount: Int = 1,
                                             maxRowCount: Int = 1000,
                                             includeSheetName: Boolean = false)

private[excel] object ExcelParserOptions {
  private val encoder = new DoubleMetaphone()

  /**
   * Valid parser options and their phonetic values
   */
  private val mappings = Map[String, String](
    encoder.encode("workbookPassword") -> "workbookPassword",
    encoder.encode("sheetNamePattern") -> "sheetNamePattern",
    encoder.encode("cellAddress") -> "cellAddress",
    encoder.encode("headerRowCount") -> "headerRowCount",
    encoder.encode("maxRowCount") -> "maxRowCount",
    encoder.encode("includeSheetName") -> "includeSheetName"
  )

  /**
   * Checks the provided set of keys for invalid options and attempts to match again
   * valid options.
   * @param keys collection of keys to valid
   * @return An [[Option]] containing a string if there are errors, or [[None]]
   */
  private def checkInvalidOptions(keys: Set[String]): Option[Seq[String]] = {
    val buffer = new ArrayBuffer[String]()

    keys.foreach(key => {
      val encodedKey = encoder.encode(key)
      if (!mappings.values.exists(_.compareToIgnoreCase(key) == 0) && mappings.contains(encodedKey)) {
        buffer.append(s"Invalid option '$key', did you mean '${mappings(encodedKey)}'?")
      }
    })

    if (buffer.isEmpty) None else Some(buffer)
  }

  /**
   * Creates a new [[ExcelParserOptions]] object from a [[CaseInsensitiveStringMap]] collection
   *
   * @param options the map of options to read from
   * @return an [[ExcelParserOptions]] object
   */
  def from(options: CaseInsensitiveStringMap): ExcelParserOptions = {
    checkInvalidOptions(options.keySet().toSet) match {
      case Some(errors) => throw new ExcelParserOptionsException(s"Unable to parse options:\n${errors.mkString("\n")}")
      case _ =>
    }

    val worksheetPassword = if (options.containsKey("workbookPassword")) {
      Some(options.get("workbookPassword"))
    } else {
      None
    }

    ExcelParserOptions(
      worksheetPassword,
      options.getOrDefault("sheetNamePattern", ""),
      options.getOrDefault("cellAddress", "A1"),
      options.getInt("headerRowCount", 1),
      options.getInt("maxRowCount", 1000),
      options.getBoolean("includeSheetName", false)
    )
  }

  /**
   * Creates a new [[ExcelParserOptions]] object from a [[Map]] collection
   *
   * @param options the map of options to read from
   * @return an [[ExcelParserOptions]] object
   */
  def from(options: Map[String, String]): ExcelParserOptions = {
    checkInvalidOptions(options.keySet) match {
      case Some(errors) => throw new ExcelParserOptionsException(s"Unable to parse options:\n${errors.mkString("\n")}")
      case _ =>
    }

    val worksheetPassword = if (options.keys.exists(_.compareToIgnoreCase("workbookPassword") == 0)) {
      Some(options("workbookPassword"))
    } else {
      None
    }

    ExcelParserOptions(
      worksheetPassword,
      options.getOrElse("sheetNamePattern", ""),
      options.getOrElse("cellAddress", "A1"),
      options.getOrElse("headerRowCount", "1").toInt,
      options.getOrElse("maxRowCount", "1000").toInt,
      options.getOrElse("includeSheetName", "false").toBoolean
    )
  }
}