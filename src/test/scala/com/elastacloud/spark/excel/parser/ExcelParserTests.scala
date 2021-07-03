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

package com.elastacloud.spark.excel.parser

import com.elastacloud.spark.excel.ExcelParserOptions
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{FileInputStream, InputStream}
import java.sql.Timestamp

class ExcelParserTests extends AnyFlatSpec with Matchers {
  private def getTestFileStream(relativePath: String): InputStream = {
    val path = getClass.getResource(relativePath).getPath
    new FileInputStream(path)
  }

  private def withInputStream(relativePath: String)(f: InputStream => Unit): Unit = {
    val inputStream = getTestFileStream(relativePath)

    try {
      f(inputStream)
    } finally {
      inputStream.close()
    }
  }

  implicit class StringExtension(val s: String) {
    def asUnsafe: UTF8String = UTF8String.fromString(s)
  }

  "Opening a standard workbook" should "open the workbook and default to the first sheet using default options" in {
    withInputStream("/Parser/SimpleWorkbook.xlsx") { inputStream =>
      val options = ExcelParserOptions()

      val parser = new ExcelParser(inputStream, options)
      parser.sheetIndexes should equal(Seq(0))
    }
  }

  it should "throw an error if there are no matching sheets" in {
    withInputStream("/Parser/SimpleWorkbook.xlsx") { inputStream =>
      val options = ExcelParserOptions(sheetNamePattern = "SheetX")
      assertThrows[ExcelParserException] {
        new ExcelParser(inputStream, options)
      }
    }
  }

  it should "generate a valid schema from the worksheet" in {
    withInputStream("/Parser/SimpleWorkbook.xlsx") { inputStream =>
      val options = ExcelParserOptions()

      val expectedSchema = StructType(Array(
        StructField("Col1", StringType, nullable = true),
        StructField("Col2", DoubleType, nullable = true),
        StructField("Col3", StringType, nullable = true)
      ))

      val parser = new ExcelParser(inputStream, options)
      parser.readDataSchema() should equal(expectedSchema)
    }
  }

  it should "return all data from the first worksheet" in {
    withInputStream("/Parser/SimpleWorkbook.xlsx") { inputStream =>
      val options = ExcelParserOptions()

      val expectedData = Seq(
        Vector[Any]("a".asUnsafe, 1D, "x".asUnsafe),
        Vector[Any]("b".asUnsafe, 2D, "y".asUnsafe),
        Vector[Any]("c".asUnsafe, 3D, "z".asUnsafe)
      )

      val parser = new ExcelParser(inputStream, options)
      val actualData = parser.getDataIterator.toList

      actualData should equal(expectedData)
    }
  }

  it should "read a subset of data given a different starting location" in {
    withInputStream("/Parser/SimpleWorkbook.xlsx") { inputStream =>
      val options = ExcelParserOptions(cellAddress = "B1")

      val expectedSchema = StructType(Array(
        StructField("Col2", DoubleType, nullable = true),
        StructField("Col3", StringType, nullable = true)
      ))

      val expectedData = Seq(
        Vector[Any](1D, "x".asUnsafe),
        Vector[Any](2D, "y".asUnsafe),
        Vector[Any](3D, "z".asUnsafe)
      )

      val parser = new ExcelParser(inputStream, options)

      parser.readDataSchema() should equal(expectedSchema)
      parser.getDataIterator.toList should equal(expectedData)
    }
  }

  "Opening a password protected workbook" should "succeed with a valid password" in {
    withInputStream("/Parser/PasswordProtectedWorkbook.xlsx") { inputStream =>
      val options = ExcelParserOptions(workbookPassword = Some("password"))

      val parser = new ExcelParser(inputStream, options)
      val data = parser.getDataIterator.toList

      data.length should be(3)
    }
  }

  "Opening a workbook with multiple sheets" should "only process the first sheet by default" in {
    withInputStream("/Parser/MultiSheetHeaderWorkbook.xlsx") { inputStream =>
      val options = ExcelParserOptions(cellAddress = "A3", headerRowCount = 0)

      val parser = new ExcelParser(inputStream, options)
      val data = parser.getDataIterator.toList

      data.length should be(3)
    }
  }

  it should "access all sheets with a provided sheet name pattern" in {
    withInputStream("/Parser/MultiSheetHeaderWorkbook.xlsx") { inputStream =>
      val options = ExcelParserOptions(cellAddress = "A3", headerRowCount = 0, sheetNamePattern = """\d{4}""")

      val parser = new ExcelParser(inputStream, options)
      val data = parser.getDataIterator.toList

      parser.sheetIndexes.length should be(2)
      data.length should be(6)
    }
  }

  "Opening a workbook with a multiline header" should "read in all parts of the header when specified" in {
    withInputStream("/Parser/MultiSheetHeaderWorkbook.xlsx") { inputStream =>
      val options = ExcelParserOptions(headerRowCount = 2)

      val parser = new ExcelParser(inputStream, options)
      val data = parser.getDataIterator.toList

      data.length should be(3)

      parser.readDataSchema().fields(0).name should be("Category")
      parser.readDataSchema().fields(0).dataType should be(StringType)

      parser.readDataSchema().fields(1).name should be("Jan_Purchased")
      parser.readDataSchema().fields(1).dataType should be(DoubleType)

      parser.readDataSchema().fields(24).name should be("Dec_Sold")
      parser.readDataSchema().fields(24).dataType should be(DoubleType)
    }
  }

  it should "read all matching sheets and apply the same schema" in {
    withInputStream("/Parser/MultiSheetHeaderWorkbook.xlsx") { inputStream =>
      val options = ExcelParserOptions(headerRowCount = 2, sheetNamePattern = """\d{4}""")

      val parser = new ExcelParser(inputStream, options)
      val data = parser.getDataIterator.toList

      val expectedColumns = Array(
        "Category",
        "Jan_Purchased",
        "Jan_Sold",
        "Feb_Purchased",
        "Feb_Sold",
        "Mar_Purchased",
        "Mar_Sold",
        "Apr_Purchased",
        "Apr_Sold",
        "May_Purchased",
        "May_Sold",
        "Jun_Purchased",
        "Jun_Sold",
        "Jul_Purchased",
        "Jul_Sold",
        "Aug_Purchased",
        "Aug_Sold",
        "Sep_Purchased",
        "Sep_Sold",
        "Oct_Purchased",
        "Oct_Sold",
        "Nov_Purchased",
        "Nov_Sold",
        "Dec_Purchased",
        "Dec_Sold"
      )

      data.length should be(6)
      parser.readDataSchema().fields.map(f => f.name) should equal(expectedColumns)
    }
  }

  "Opening a workbook with formula" should "return calculated values" in {
    withInputStream("/Parser/CalculatedData.xlsx") { inputStream =>
      val expectedSchema = StructType(Array(
        StructField("Col_A", DoubleType, nullable = true),
        StructField("Col_B", TimestampType, nullable = true),
        StructField("Col_C", DoubleType, nullable = true),
        StructField("Col_D", TimestampType, nullable = true),
        StructField("Col_E", DoubleType, nullable = true)
      ))

      val expectedRowData = Seq[Any](
        6,
        DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf("2020-01-06 00:00:00.000")),
        36,
        DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf("2020-04-30 00:00:00.000")),
        6
      )

      val parser = new ExcelParser(inputStream, ExcelParserOptions())
      parser.readDataSchema() should equal(expectedSchema)

      val data = parser.getDataIterator.toList
      data.length should be(10)
      data(5) should equal(expectedRowData)
    }
  }

  "Opening a workbook with blank cells" should "continue to be read without error" in {
    withInputStream("/Parser/SimpleWorkbookWithBlanks.xlsx") { inputStream =>
      val options = ExcelParserOptions()

      val expectedData = Seq(
        Vector[Any]("a".asUnsafe, 1D, "x".asUnsafe),
        Vector[Any]("b".asUnsafe, null, "y".asUnsafe),
        Vector[Any]("c".asUnsafe, 3D, "z".asUnsafe)
      )

      val parser = new ExcelParser(inputStream, options)
      val actualData = parser.getDataIterator.toList

      actualData should equal(expectedData)
    }
  }

  "Opening a workbook with data starting at an offset" should "throw an error if the default cell address issued" in {
    withInputStream("/Parser/MisalignedTable.xlsx") { inputStream =>
      val options = ExcelParserOptions()

      val parser = new ExcelParser(inputStream, options)
      the[ExcelParserException] thrownBy parser.readDataSchema() should have message "No data found on first row"
    }
  }

  it should "return the correct data when a valid starting position is defined" in {
    withInputStream("/Parser/MisalignedTable.xlsx") { inputStream =>
      val options = ExcelParserOptions(cellAddress = "C5")

      val expectedData = Seq(
        Vector[Any]("a".asUnsafe, 1D, "x".asUnsafe),
        Vector[Any]("b".asUnsafe, 2D, "y".asUnsafe),
        Vector[Any]("c".asUnsafe, 3D, "z".asUnsafe)
      )

      val parser = new ExcelParser(inputStream, options)
      parser.getDataIterator.toList should equal(expectedData)
    }
  }

  "Defining an output schema" should "filter the output from the source file" in {
    withInputStream("/Parser/SimpleWorkbook.xlsx") { inputStream =>
      val options = ExcelParserOptions()

      val dataSchema = StructType(Array(
        StructField("Col1", StringType, nullable = true),
        StructField("Col2", DoubleType, nullable = true),
        StructField("Col3", StringType, nullable = true)
      ))

      val readSchema = StructType(Array(
        StructField("Col2", DoubleType, nullable = true)
      ))

      val expectedData = Seq(
        Vector[Any](1D),
        Vector[Any](2D),
        Vector[Any](3D)
      )

      val parser = new ExcelParser(inputStream, options, Some(dataSchema), Some(readSchema))
      parser.getDataIterator.toList should equal(expectedData)
    }
  }


  it should "return valid string representations of values if the source data is non-string" in {
    withInputStream("/Parser/NonStringValues.xlsx") { inputStream =>
      val options = ExcelParserOptions()

      val dataSchema = StructType(Array(
        StructField("Number", StringType, nullable = false),
        StructField("Date", StringType, nullable = false),
        StructField("Boolean", StringType, nullable = false),
        StructField("Scientific", StringType, nullable = false)
      ))

      val expectedData = Seq(
        Vector[Any]("1.0".asUnsafe, "2021-08-13T00:00:00".asUnsafe, "true".asUnsafe, "3.9E-10".asUnsafe),
        Vector[Any]("2.0".asUnsafe, "2021-08-14T00:00:00".asUnsafe, "false".asUnsafe, "3.1415E8".asUnsafe)
      )

      val parser = new ExcelParser(inputStream, options, Some(dataSchema), Some(dataSchema))
      val firstRow = parser.getDataIterator.toList.take(2)

      firstRow should equal(expectedData)
    }
  }
}
