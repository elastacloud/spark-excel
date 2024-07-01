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
import org.scalatest.Tag
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks.forAll
import org.scalatest.prop.Tables.Table

import java.io.{FileInputStream, InputStream}
import java.sql.{Date, Timestamp}

object LongRunningTest extends Tag("LongRunningTest")

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

  private val useStreamingTable = Table(
    "false",
    "true"
  )

  "Opening a standard workbook" should "open the workbook and default to the first sheet using default options" in {
    forAll(useStreamingTable) { useStreaming =>
      withInputStream("/Parser/SimpleWorkbook.xlsx") { inputStream =>
        val options = new ExcelParserOptions(Map[String, String]{
          "useStreaming" -> useStreaming
        })

        val parser = new ExcelParser(inputStream, options)
        parser.sheetIndexes should equal(Seq(0))
      }
    }
  }

  it should "throw an error if there are no matching sheets" in {
    forAll(useStreamingTable) { useStreaming =>
      withInputStream("/Parser/SimpleWorkbook.xlsx") { inputStream =>
        val options = new ExcelParserOptions(Map[String, String](
          "sheetNamePattern" -> "SheetX",
          "useStreaming" -> useStreaming
        ))
        assertThrows[ExcelParserException] {
          new ExcelParser(inputStream, options)
        }
      }
    }
  }

  it should "generate a valid schema from the worksheet" in {
    forAll(useStreamingTable) { useStreaming =>
      withInputStream("/Parser/SimpleWorkbook.xlsx") { inputStream =>
        val options = new ExcelParserOptions(Map[String, String] {
          "useStreaming" -> useStreaming
        })

        val expectedSchema = StructType(Array(
          StructField("Col1", StringType, nullable = true),
          StructField("Col2", DoubleType, nullable = true),
          StructField("Col3", StringType, nullable = true)
        ))

        val parser = new ExcelParser(inputStream, options)
        parser.readDataSchema() should equal(expectedSchema)
      }
    }
  }

  it should "return all data from the first worksheet" in {
    forAll(useStreamingTable) { useStreaming =>
      withInputStream("/Parser/SimpleWorkbook.xlsx") { inputStream =>
        val options = new ExcelParserOptions(Map[String, String] {
          "useStreaming" -> useStreaming
        })

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
  }

  it should "read a subset of data given a different starting location" in {
    forAll(useStreamingTable) { useStreaming =>
      withInputStream("/Parser/SimpleWorkbook.xlsx") { inputStream =>
        val options = new ExcelParserOptions(Map[String, String](
          "cellAddress" -> "B1",
          "useStreaming" -> useStreaming
        ))

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
  }

  it should "read data correctly using the inferred schema" in {
    forAll(useStreamingTable) { useStreaming =>
      withInputStream("/Parser/VaryingTypes.xlsx") { inputStream =>
        val options = new ExcelParserOptions(Map[String, String](
          "evaluateFormulae" -> "false",
          "maxRowCount" -> "3",
          "useStreaming" -> useStreaming
        )) // Limit the row count so that it doesn't infer based on the string row

        val expectedSchema = StructType(Array(
          StructField("Item", StringType, nullable = true),
          StructField("2010", DoubleType, nullable = true),
          StructField("2011", DoubleType, nullable = true),
          StructField("IsGood", BooleanType, nullable = true)
        ))

        val expectedData = Seq(
          Vector[Any]("Item 1".asUnsafe, 99.4, 99.4, true),
          Vector[Any]("Item 2".asUnsafe, 12.4, 12.4, true),
          Vector[Any]("Item 3".asUnsafe, 74.2, 74.2, true),
          Vector[Any]("Item 4".asUnsafe, 36.8, 36.8, false),
          Vector[Any]("Item 5".asUnsafe, 24.2, 24.2, false),
          Vector[Any]("Item 6".asUnsafe, 11.6, 11.6, false),
          Vector[Any]("Header Items".asUnsafe, null, null, null),
          Vector[Any]("Item 12".asUnsafe, 99.2, 99.2, false),
          Vector[Any]("Item 13".asUnsafe, 18.4, 18.4, true),
          Vector[Any]("Item 14".asUnsafe, 12.3, 12.3, true)
        )

        val parser = new ExcelParser(inputStream, options)
        parser.readDataSchema() should equal(expectedSchema)
        parser.getDataIterator.toList should equal(expectedData)
      }
    }
  }

  "Opening a password protected workbook" should "succeed with a valid password" in {
    forAll(useStreamingTable) { useStreaming =>
      withInputStream("/Parser/PasswordProtectedWorkbook.xlsx") { inputStream =>
        val options = new ExcelParserOptions(Map[String, String](
          "workbookPassword" -> "password",
          "useStreaming" -> useStreaming
        ))

        val parser = new ExcelParser(inputStream, options)
        val data = parser.getDataIterator.toList

        data.length should be(3)
      }
    }
  }

  "Opening a workbook with multiple sheets" should "only process the first sheet by default" in {
    forAll(useStreamingTable) { useStreaming =>
      withInputStream("/Parser/MultiSheetHeaderWorkbook.xlsx") { inputStream =>
        val options = new ExcelParserOptions(Map[String, String](
          "cellAddress" -> "A3",
          "headerRowCount" -> "0",
          "useStreaming" -> useStreaming
        ))

        val parser = new ExcelParser(inputStream, options)
        val data = parser.getDataIterator.toList

        data.length should be(3)
      }
    }
  }

  it should "access all sheets with a provided sheet name pattern" in {
    forAll(useStreamingTable) { useStreaming =>
      withInputStream("/Parser/MultiSheetHeaderWorkbook.xlsx") { inputStream =>
        val options = new ExcelParserOptions(Map[String, String](
          "cellAddress" -> "A3",
          "headerRowCount" -> "0",
          "sheetNamePattern" -> """\d{4}""",
          "useStreaming" -> useStreaming
        ))

        val parser = new ExcelParser(inputStream, options)
        val data = parser.getDataIterator.toList

        parser.sheetIndexes.length should be(2)
        data.length should be(6)
      }
    }
  }

  "Opening a workbook with a multiline header" should "read in all parts of the header when specified" in {
    forAll(useStreamingTable) { useStreaming =>
      withInputStream("/Parser/MultiSheetHeaderWorkbook.xlsx") { inputStream =>
        val options = new ExcelParserOptions(Map[String, String](
          "headerRowCount" -> "2",
          "useStreaming" -> useStreaming
        ))

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
  }

  it should "read all matching sheets and apply the same schema" in {
    forAll(useStreamingTable) { useStreaming =>
      withInputStream("/Parser/MultiSheetHeaderWorkbook.xlsx") { inputStream =>
        val options = new ExcelParserOptions(Map[String, String](
          "headerRowCount" -> "2",
          "sheetNamePattern" -> """\d{4}""",
          "useStreaming" -> useStreaming
        ))

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

      val parser = new ExcelParser(inputStream, new ExcelParserOptions())
      parser.readDataSchema() should equal(expectedSchema)

      val data = parser.getDataIterator.toList
      data.length should be(10)
      data(5) should equal(expectedRowData)
    }
  }

  it should "Return only the string value of the cell if formula evaluation is disabled" in {
    forAll(useStreamingTable) { useStreaming =>
      withInputStream("/Parser/CalculatedData.xlsx") { inputStream =>
        val expectedSchema = StructType(Array(
          StructField("Col_A", DoubleType, nullable = true),
          StructField("Col_B", TimestampType, nullable = true),
          StructField("Col_C", StringType, nullable = true),
          StructField("Col_D", StringType, nullable = true),
          StructField("Col_E", StringType, nullable = true)
        ))

        val expectedRowData = Seq[Any](
          6,
          DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf("2020-01-06 00:00:00.000")),
          "A7*A7".asUnsafe,
          "EOMONTH(B7,3)".asUnsafe,
          "DAY(B7)".asUnsafe
        )

        val options = new ExcelParserOptions(Map[String, String](
          "evaluateFormulae" -> "false",
          "useStreaming" -> useStreaming
        ))

        val parser = new ExcelParser(inputStream, options)
        parser.readDataSchema() should equal(expectedSchema)

        val data = parser.getDataIterator.toList
        data.length should be(10)
        data(5) should equal(expectedRowData)
      }
    }
  }

  it should "Handle string concatenation formulas" in {
    withInputStream("/Parser/ConcatString.xlsx") { inputStream =>
      val expectedSchema = StructType(Array(
        StructField("Title", StringType, nullable = true),
        StructField("Given_Name", StringType, nullable = true),
        StructField("Family_Name", StringType, nullable = true),
        StructField("Full_Name", StringType, nullable = true)
      ))

      val expectedData = Seq(
        Vector[Any]("Dr".asUnsafe, "Jennifer".asUnsafe, "Alagora".asUnsafe, "Dr Jennifer Alagora".asUnsafe),
        Vector[Any]("Mr".asUnsafe, "Adam".asUnsafe, "Fox".asUnsafe, "Mr Adam Fox".asUnsafe),
        Vector[Any]("Ms".asUnsafe, null, "Proctor".asUnsafe, "Ms Proctor".asUnsafe)
      )

      val parser = new ExcelParser(inputStream, new ExcelParserOptions())
      parser.readDataSchema() should equal(expectedSchema)

      val actualData = parser.getDataIterator.toList
      actualData should equal(expectedData)
    }
  }

  "Opening a workbook with blank cells" should "continue to be read without error" in {
    forAll(useStreamingTable) { useStreaming =>
      withInputStream("/Parser/SimpleWorkbookWithBlanks.xlsx") { inputStream =>
        val options = new ExcelParserOptions(Map[String, String]{
          "useStreaming" -> useStreaming
        })

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
  }

  "Opening a workbook with data starting at an offset" should "return the correct data when a valid starting position is defined" in {
    forAll(useStreamingTable) { useStreaming =>
      withInputStream("/Parser/MisalignedTable.xlsx") { inputStream =>
        val options = new ExcelParserOptions(Map[String, String](
          "cellAddress" -> "C5",
          "useStreaming" -> useStreaming
        ))

        val expectedData = Seq(
          Vector[Any]("a".asUnsafe, 1D, "x".asUnsafe),
          Vector[Any]("b".asUnsafe, 2D, "y".asUnsafe),
          Vector[Any]("c".asUnsafe, 3D, "z".asUnsafe)
        )

        val parser = new ExcelParser(inputStream, options)
        parser.getDataIterator.toList should equal(expectedData)
      }
    }
  }

  "Defining an output schema" should "filter the output from the source file" in {
    forAll(useStreamingTable) { useStreaming =>
      withInputStream("/Parser/SimpleWorkbook.xlsx") { inputStream =>
        val options = new ExcelParserOptions(Map[String, String] {
          "useStreaming" -> useStreaming
        })

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
  }

  it should "handle numeric values where the requested schema type is integer or long" in {
    forAll(useStreamingTable) { useStreaming =>
      withInputStream("/Parser/VaryingTypes.xlsx") { inputStream =>
        val options = new ExcelParserOptions(Map[String, String]{
          "useStreaming" -> useStreaming
        })

        val dataSchema = StructType(Array(
          StructField("Item", StringType, nullable = true),
          StructField("2010", IntegerType, nullable = true),
          StructField("2011", LongType, nullable = true)
        ))

        val expectedData = Seq(
          Vector[Any]("Item 1".asUnsafe, 99, 99L),
          Vector[Any]("Item 2".asUnsafe, 12, 12L),
          Vector[Any]("Item 3".asUnsafe, 74, 74L),
          Vector[Any]("Item 4".asUnsafe, 36, 36L),
          Vector[Any]("Item 5".asUnsafe, 24, 24L),
          Vector[Any]("Item 6".asUnsafe, 11, 11L),
          Vector[Any]("Header Items".asUnsafe, null, null),
          Vector[Any]("Item 12".asUnsafe, 99, 99L),
          Vector[Any]("Item 13".asUnsafe, 18, 18L),
          Vector[Any]("Item 14".asUnsafe, 12, 12L)
        )

        val parser = new ExcelParser(inputStream, options, Some(dataSchema), Some(dataSchema))
        parser.getDataIterator.toList should equal(expectedData)
      }
    }
  }

  it should "handle numeric values where the requested schema type is date, float, or double" in {
    withInputStream("/Parser/CalculatedData.xlsx") { inputStream =>
      val options = new ExcelParserOptions()

      val dataSchema = StructType(Array(
        StructField("Col_A", IntegerType, nullable = true),
        StructField("Col_B", DateType, nullable = true),
        StructField("Col_C", FloatType, nullable = true),
        StructField("Col_D", DateType, nullable = true),
        StructField("Col_E", DoubleType, nullable = true)
      ))

      val expectedData = Vector[Any](
        1,
        DateTimeUtils.fromJavaDate(Date.valueOf("2020-01-01")),
        1F,
        DateTimeUtils.fromJavaDate(Date.valueOf("2020-04-30")),
        1D
      )

      val parser = new ExcelParser(inputStream, options, Some(dataSchema), Some(dataSchema))
      parser.getDataIterator.toList.head should equal(expectedData)
    }
  }

  it should "return valid string representations of values if the source data is non-string" in {
    forAll(useStreamingTable) { useStreaming =>
      withInputStream("/Parser/NonStringValues.xlsx") { inputStream =>
        val options = new ExcelParserOptions(Map[String, String]{
          "useStreaming" -> useStreaming
        })

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

  "Specifying a schema match column" should "add the column to the inferred schema" in {
    forAll(useStreamingTable) { useStreaming =>
      withInputStream("/Parser/SimpleWorkbook.xlsx") { inputStream =>
        val options = new ExcelParserOptions(Map[String, String](
          "cellAddress" -> "B1",
          "schemaMatchColumnName" -> "_isValid",
          "useStreaming" -> useStreaming
        ))

        val expectedSchema = StructType(Array(
          StructField("Col2", DoubleType, nullable = true),
          StructField("Col3", StringType, nullable = true),
          StructField("_isValid", BooleanType, nullable = false)
        ))

        val expectedData = Seq(
          Vector[Any](1D, "x".asUnsafe, true),
          Vector[Any](2D, "y".asUnsafe, true),
          Vector[Any](3D, "z".asUnsafe, true)
        )

        val parser = new ExcelParser(inputStream, options)
        val actualData = parser.getDataIterator.toList

        parser.readDataSchema() should equal(expectedSchema)
        actualData should equal(expectedData)
      }
    }
  }

  it should "throw an exception if the specified name exists in the data set" in {
    forAll(useStreamingTable) { useStreaming =>
      withInputStream("/Parser/SimpleWorkbook.xlsx") { inputStream =>
        val options = new ExcelParserOptions(Map[String, String](
          "schemaMatchColumnName" -> "Col3",
          "useStreaming" -> useStreaming
        ))

        val parser = new ExcelParser(inputStream, options)

        the[ExcelParserException] thrownBy parser.readDataSchema() should have message "The specified schema match column conflicts with a column of the same name in the data set."
      }
    }
  }

  it should "flag rows as false if the data types do not match the inferred schema" in {
    forAll(useStreamingTable) { useStreaming =>
      withInputStream("/Parser/VaryingTypes.xlsx") { inputStream =>
        // Limit the row count so that it doesn't infer based on the string row
        val options = new ExcelParserOptions(Map[String, String](
          "maxRowCount" -> "3",
          "schemaMatchColumnName" -> "ValidRow",
          "useStreaming" -> useStreaming
        ))

        val expectedSchema = StructType(Array(
          StructField("Item", StringType, nullable = true),
          StructField("2010", DoubleType, nullable = true),
          StructField("2011", DoubleType, nullable = true),
          StructField("IsGood", BooleanType, nullable = true),
          StructField("ValidRow", BooleanType, nullable = false)
        ))

        val expectedData = Seq(
          Vector[Any]("Item 1".asUnsafe, 99.4, 99.4, true, true),
          Vector[Any]("Item 2".asUnsafe, 12.4, 12.4, true, true),
          Vector[Any]("Item 3".asUnsafe, 74.2, 74.2, true, true),
          Vector[Any]("Item 4".asUnsafe, 36.8, 36.8, false, true),
          Vector[Any]("Item 5".asUnsafe, 24.2, 24.2, false, true),
          Vector[Any]("Item 6".asUnsafe, 11.6, 11.6, false, true),
          Vector[Any]("Header Items".asUnsafe, null, null, null, false),
          Vector[Any]("Item 12".asUnsafe, 99.2, 99.2, false, true),
          Vector[Any]("Item 13".asUnsafe, 18.4, 18.4, true, true),
          Vector[Any]("Item 14".asUnsafe, 12.3, 12.3, true, true)
        )

        val parser = new ExcelParser(inputStream, options)
        parser.readDataSchema() should equal(expectedSchema)
        parser.getDataIterator.toList should equal(expectedData)
      }
    }
  }

  it should "use the provided field when a schema is provided" in {
    forAll(useStreamingTable) { useStreaming =>
      withInputStream("/Parser/SimpleWorkbook.xlsx") { inputStream =>
        val options = new ExcelParserOptions(Map[String, String](
          "schemaMatchColumnName" -> "MatchesSchema",
          "useStreaming" -> useStreaming
        ))

        val schema = new StructType(Array(
          StructField("Col1", StringType, nullable = false),
          StructField("Col2", IntegerType, nullable = false),
          StructField("Col3", DoubleType, nullable = false),
          StructField("MatchesSchema", BooleanType, nullable = false)
        ))

        val expectedData = Seq(
          Vector[Any]("a".asUnsafe, 1, null, false),
          Vector[Any]("b".asUnsafe, 2, null, false),
          Vector[Any]("c".asUnsafe, 3, null, false)
        )

        val parser = new ExcelParser(inputStream, options, schema = Some(schema))
        val actualData = parser.getDataIterator.toList

        actualData should equal(expectedData)
      }
    }
  }

  it should "throw an error if the option column name is not in the schema" in {
    forAll(useStreamingTable) { useStreaming =>
      withInputStream("/Parser/SimpleWorkbook.xlsx") { inputStream =>
        val options = new ExcelParserOptions(Map[String, String](
          "schemaMatchColumnName" -> "MatchesSchema",
          "useStreaming" -> useStreaming
        ))

        val schema = new StructType(Array(
          StructField("Col1", StringType, nullable = false),
          StructField("Col2", IntegerType, nullable = false),
          StructField("Col3", DoubleType, nullable = false),
          StructField("_isValid", BooleanType, nullable = false)
        ))

        val parser = new ExcelParser(inputStream, options, schema = Some(schema))

        the[ExcelParserException] thrownBy parser.getDataIterator.toList should have message "The specified schema match column does not exist within the schema."
      }
    }
  }

  it should "throw an error if the option column name is not of the correct data type" in {
    forAll(useStreamingTable) { useStreaming =>
      withInputStream("/Parser/SimpleWorkbook.xlsx") { inputStream =>
        val options = new ExcelParserOptions(Map[String, String](
          "schemaMatchColumnName" -> "_isValid",
          "useStreaming" -> useStreaming
        ))

        val schema = new StructType(Array(
          StructField("Col1", StringType, nullable = false),
          StructField("Col2", IntegerType, nullable = false),
          StructField("Col3", DoubleType, nullable = false),
          StructField("_isValid", LongType, nullable = true)
        ))

        val parser = new ExcelParser(inputStream, options, schema = Some(schema))

        the[ExcelParserException] thrownBy parser.getDataIterator.toList should have message "The specified schema match column is not defined as a boolean type."
      }
    }
  }

  "Specifying a null value" should "read the string value as null" in {
    forAll(useStreamingTable) { useStreaming =>
      withInputStream("/Parser/SimpleWorkbook.xlsx") { inputStream =>
        val options = new ExcelParserOptions(Map[String, String](
          "nullValue" -> "y",
          "useStreaming" -> useStreaming
        ))

        val expectedData = Seq(
          Vector[Any]("a".asUnsafe, 1D, "x".asUnsafe),
          Vector[Any]("b".asUnsafe, 2D, null),
          Vector[Any]("c".asUnsafe, 3D, "z".asUnsafe)
        )

        val parser = new ExcelParser(inputStream, options)
        val actualData = parser.getDataIterator.toList

        actualData should equal(expectedData)
      }
    }
  }

  it should "Handle string concatenation formulas" in {
    withInputStream("/Parser/ConcatString.xlsx") { inputStream =>
      val options = new ExcelParserOptions(Map[String, String](
        "nullValue" -> "MR ADAM FOX"
      ))

      val expectedData = Seq(
        Vector[Any]("Dr".asUnsafe, "Jennifer".asUnsafe, "Alagora".asUnsafe, "Dr Jennifer Alagora".asUnsafe),
        Vector[Any]("Mr".asUnsafe, "Adam".asUnsafe, "Fox".asUnsafe, null),
        Vector[Any]("Ms".asUnsafe, null, "Proctor".asUnsafe, "Ms Proctor".asUnsafe)
      )

      val parser = new ExcelParser(inputStream, options)

      val actualData = parser.getDataIterator.toList
      actualData should equal(expectedData)
    }
  }

  "Reading a file containing no data" should "throw an exception" in {
    forAll(useStreamingTable) { useStreaming =>
      withInputStream("/Parser/Empty.xlsx") { inputStream =>
        val parser = new ExcelParser(inputStream, new ExcelParserOptions(Map[String, String]{
          "useStreaming" -> useStreaming
        }))

        val error = intercept[ExcelParserException] {
          parser.getDataIterator.toList
        }

        error.getMessage should be("No data found")
      }
    }
  }

  it should "return an single empty record if only headers exist" in {
    forAll(useStreamingTable) { useStreaming =>
      withInputStream("/Parser/NoData.xlsx") { inputStream =>
        val expectedData = Seq(Vector(null, null, null))

        val parser = new ExcelParser(inputStream, new ExcelParserOptions(Map[String, String]{
          "useStreaming" -> useStreaming
        }))
        val actualData = parser.getDataIterator.toList

        actualData should be(expectedData)
      }
    }
  }

  "Reading using streaming" should "throw an error for XLS formats" in {
    withInputStream("/Parser/SimpleWorkbook.xls") { inputStream =>
      val options = new ExcelParserOptions(Map[String, String](
        "useStreaming" -> "true"
      ))

      val error = intercept[ExcelParserException] {
        new ExcelParser(inputStream, options)
      }

      error.getMessage should be("Unable to open non-xlsx files in streaming mode")
    }
  }

  it should "not result in GC collection issues for larger files" taggedAs LongRunningTest in {
    withInputStream("/Parser/parking-citations-smaller.xlsx") { inputStream =>
      val options = new ExcelParserOptions(Map[String, String](
        "useStreaming" -> "true"
      ))

      val expectedSchema = new StructType(Array(
        StructField("Ticket_number", DoubleType, nullable = true),
        StructField("Issue_Date", StringType, nullable = true),
        StructField("Issue_time", DoubleType, nullable = true),
        StructField("Meter_Id", StringType, nullable = true),
        StructField("Marked_Time", DoubleType, nullable = true),
        StructField("RP_State_Plate", StringType, nullable = true),
        StructField("Plate_Expiry_Date", DoubleType, nullable = true),
        StructField("VIN", StringType, nullable = true),
        StructField("Make", StringType, nullable = true),
        StructField("Body_Style", StringType, nullable = true),
        StructField("Color", StringType, nullable = true),
        StructField("Location", StringType, nullable = true),
        StructField("Route", StringType, nullable = true),
        StructField("Agency", DoubleType, nullable = true),
        StructField("Violation_code", StringType, nullable = true),
        StructField("Violation_Description", StringType, nullable = true),
        StructField("Fine_amount", DoubleType, nullable = true),
        StructField("Latitude", DoubleType, nullable = true),
        StructField("Longitude", DoubleType, nullable = true)
      ))

      val expectedRecord = Vector[Any](
        4357210690D,
        "2019-07-30T00:00:00.000".asUnsafe,
        1803D,
        null,
        null,
        "CA".asUnsafe,
        202008D,
        null,
        "VOLK".asUnsafe,
        "PA".asUnsafe,
        "WT".asUnsafe,
        "1245 FACTORY PL".asUnsafe,
        "00600".asUnsafe,
        56D,
        "80.69B".asUnsafe,
        "NO PARKING".asUnsafe,
        73D,
        6489735.158943,
        1836708.022147
      )

      val parser = new ExcelParser(inputStream, options)
      val actualData = parser.getDataIterator.toList
      val schema = parser.excelSchema

      actualData.length should be(300593)
      actualData.last.head should be(4354211083.0)
      actualData(999) should equal(expectedRecord)
      schema.get should equal(expectedSchema)
    }
  }
}
