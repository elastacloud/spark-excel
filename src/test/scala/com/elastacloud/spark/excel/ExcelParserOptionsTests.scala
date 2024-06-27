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

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters.mapAsJavaMapConverter

class ExcelParserOptionsTests extends AnyFlatSpec with Matchers {
  "Creating a default instance" should "use default values" in {
    val options = new ExcelParserOptions()

    options.workbookPassword should be(None)
    options.sheetNamePattern shouldBe empty
    options.cellAddress should be("A1")
    options.headerRowCount should be(1)
    options.maxRowCount should be(1000)
    options.includeSheetName should be(false)
    options.nulLValue should be(None)
    options.thresholdBytesForTempFiles should be(100000000)
    options.schemaMatchColumnName should be(null)
    options.evaluateFormulae should be(true)
  }

  "Creating from a case insensitive map" should "use default values for an empty map" in {
    val input = new CaseInsensitiveStringMap(Map[String, String]().asJava)

    val options = ExcelParserOptions.from(input)

    options.workbookPassword should be(None)
    options.sheetNamePattern shouldBe empty
    options.cellAddress should be("A1")
    options.headerRowCount should be(1)
    options.maxRowCount should be(1000)
    options.includeSheetName should be(false)
    options.nulLValue should be(None)
    options.thresholdBytesForTempFiles should be(100000000)
    options.schemaMatchColumnName should be(null)
    options.evaluateFormulae should be(true)
  }

  it should "extract values from the map" in {
    val input = new CaseInsensitiveStringMap(Map[String, String](
      "workbookPassword" -> "abc123",
      "sheetNamePattern" -> "Sheet[12]",
      "cellAddress" -> "B3",
      "headerRowCount" -> "12",
      "maxRowCount" -> "2000",
      "includeSheetName" -> "true",
      "nullValue" -> "NA",
      "maxBytesForTempFiles" -> "10",
      "schemaMatchColumnName" -> "_isValid",
      "evaluateFormulae" -> "false"
    ).asJava)

    val options = ExcelParserOptions.from(input)

    options.workbookPassword should be(Some("abc123"))
    options.sheetNamePattern should be("Sheet[12]")
    options.cellAddress should be("B3")
    options.headerRowCount should be(12)
    options.maxRowCount should be(2000)
    options.includeSheetName should be(true)
    options.nulLValue should be(Some("NA"))
    options.thresholdBytesForTempFiles should be(10)
    options.schemaMatchColumnName should be("_isValid")
    options.evaluateFormulae should be(false)
  }

  it should "provide useful error information if options are slightly mis-spelt" in {
    val input = new CaseInsensitiveStringMap(Map[String, String](
      "workbookPasword" -> "abc123",
      "sheetNamPatten" -> "Sheet[12]",
      "cellAdres" -> "B3",
      "headerCount" -> "12",
      "maxRowCont" -> "2000",
      "includShetNam" -> "true",
      "nulvalue" -> "NA",
      "macsBitesTempFiles" -> "10",
      "schemaMatchColumName" -> "_isValid",
      "evalateFormula" -> "false"
    ).asJava)

    val exception = the[ExcelParserOptionsException] thrownBy ExcelParserOptions.from(input)

    exception.getMessage.contains("Invalid option 'workbookpasword', did you mean 'workbookPassword'?") should be(true)
    exception.getMessage.contains("Invalid option 'sheetnampatten', did you mean 'sheetNamePattern'?") should be(true)
    exception.getMessage.contains("Invalid option 'celladres', did you mean 'cellAddress'?") should be(true)
    exception.getMessage.contains("Invalid option 'headercount', did you mean 'headerRowCount'?") should be(true)
    exception.getMessage.contains("Invalid option 'maxrowcont', did you mean 'maxRowCount'?") should be(true)
    exception.getMessage.contains("Invalid option 'includshetnam', did you mean 'includeSheetName'?") should be(true)
    exception.getMessage.contains("Invalid option 'nulvalue', did you mean 'nullValue'?") should be(true)
    exception.getMessage.contains("Invalid option 'macsbitestempfiles', did you mean 'maxBytesForTempFiles'") should be(true)
    exception.getMessage.contains("Invalid option 'schemamatchcolumname', did you mean 'schemaMatchColumnName'") should be(true)
    exception.getMessage.contains("Invalid option 'evalateformula', did you mean 'evaluateFormulae'") should be(true)
  }

  it should "ignore options which are invalid and not close in spelling to valid options" in {
    val input = new CaseInsensitiveStringMap(Map[String, String](
      "pwd" -> "abc123"
    ).asJava)

    val options = ExcelParserOptions.from(input)

    options.workbookPassword should be(None)
  }

  it should "use thresholdBytesForTempFiles if maxBytesForTempFiles is not provided" in {
    val input = new CaseInsensitiveStringMap(Map[String, String](
      "thresholdBytesForTempFiles" -> "100"
    ).asJava)

    val options = ExcelParserOptions.from(input)

    options.thresholdBytesForTempFiles should be(100)
  }

  it should "use thresholdBytesForTempFiles if maxBytesForTempFiles is alo specified" in {
    val input = new CaseInsensitiveStringMap(Map[String, String](
      "thresholdBytesForTempFiles" -> "100",
      "maxBytesForTempFiles" -> "120"
    ).asJava)

    val options = ExcelParserOptions.from(input)

    options.thresholdBytesForTempFiles should be(100)
  }

  "Creating from a string map" should "use default values for an empty map" in {
    val input = Map[String, String]()

    val options = ExcelParserOptions.from(input)

    options.workbookPassword should be(None)
    options.sheetNamePattern shouldBe empty
    options.cellAddress should be("A1")
    options.headerRowCount should be(1)
    options.maxRowCount should be(1000)
    options.includeSheetName should be(false)
    options.nulLValue should be(None)
    options.thresholdBytesForTempFiles should be(100000000)
    options.schemaMatchColumnName should be(null)
    options.evaluateFormulae should be(true)
  }

  it should "extract values from the map" in {
    val input = Map[String, String](
      "workbookPassword" -> "abc123",
      "sheetNamePattern" -> "Sheet[12]",
      "cellAddress" -> "B3",
      "headerRowCount" -> "12",
      "maxRowCount" -> "2000",
      "includeSheetName" -> "true",
      "nullValue" -> "NA",
      "maxBytesForTempFiles" -> "100",
      "schemaMatchColumnName" -> "_isValid",
      "evaluateFormulae" -> "false"
    )

    val options = ExcelParserOptions.from(input)

    options.workbookPassword should be(Some("abc123"))
    options.sheetNamePattern should be("Sheet[12]")
    options.cellAddress should be("B3")
    options.headerRowCount should be(12)
    options.maxRowCount should be(2000)
    options.includeSheetName should be(true)
    options.nulLValue should be(Some("NA"))
    options.thresholdBytesForTempFiles should be(100)
    options.schemaMatchColumnName should be("_isValid")
    options.evaluateFormulae should be(false)
  }

  it should "use thresholdBytesForTempFiles if maxBytesForTempFiles is not provided" in {
    val input = Map[String, String](
      "thresholdBytesForTempFiles" -> "100"
    )

    val options = ExcelParserOptions.from(input)

    options.thresholdBytesForTempFiles should be(100)
  }

  it should "use thresholdBytesForTempFiles if maxBytesForTempFiles is alo specified" in {
    val input = Map[String, String](
      "thresholdBytesForTempFiles" -> "100",
      "maxBytesForTempFiles" -> "120"
    )

    val options = ExcelParserOptions.from(input)

    options.thresholdBytesForTempFiles should be(100)
  }

  it should "throw an exception if no value is provided for the schemaMatchColumnName option" in {
    val input = Map[String, String](
      "schemaMatchColumnName" -> "  "
    )

    val exception = the[ExcelParserOptionsException] thrownBy ExcelParserOptions.from(input)
    exception.getMessage should be("The 'schemaMatchColumnName' option must contain a value if provided")
  }

  "Setting properties for options" should "correctly assign the values to the options class" in {
    val optionsMap = Map[String, String](
      "workbookPassword" -> "abc123",
      "sheetNamePattern" -> """\d{2,3}""",
      "cellAddress" -> "H8",
      "headerRowCount" -> "17",
      "maxRowCount" -> "5",
      "includeSheetName" -> "true",
      "nullValue" -> "N/A",
      "thresholdBytesForTempFiles" -> "12",
      "schemaMatchColumnName" -> "matchesSchema",
      "evaluateFormulae" -> "false"
    )

    val options = new ExcelParserOptions(optionsMap)

    options.workbookPassword should be(Some("abc123"))
    options.sheetNamePattern should be("""\d{2,3}""")
    options.cellAddress should be("H8")
    options.headerRowCount should be(17)
    options.maxRowCount should be(5)
    options.includeSheetName should be(true)
    options.nulLValue should be(Some("N/A"))
    options.thresholdBytesForTempFiles should be(12)
    options.schemaMatchColumnName should be("matchesSchema")
    options.evaluateFormulae should be(false)
  }
}
