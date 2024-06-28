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

import com.elastacloud.spark.excel.parser.ExcelParserException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DefaultSourceTests extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  private def testFilePath(relativePath: String): String = getClass.getResource(relativePath).getPath

  lazy val spark: SparkSession = SparkSession.builder()
    .appName("DefaultSourceTests")
    .master("local[2]")
    .getOrCreate()

  override def afterAll(): Unit = {
    spark.close()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()

    super.afterAll()
  }

  private val simpleWorkbookSchema = StructType(Array(
    StructField("Col1", StringType, nullable = true),
    StructField("Col2", DoubleType, nullable = true),
    StructField("Col3", StringType, nullable = true)
  ))

  "Opening a standard workbook" should "result in all data being read using default options" in {
    val inputPath = testFilePath("/Parser/SimpleWorkbook.xlsx")

    val df = spark.read.format("com.elastacloud.spark.excel").load(inputPath)

    df.count() should be(3)
    df.schema should equal(simpleWorkbookSchema)
  }

  it should "output the sheet name if requested via options" in {
    val inputPath = testFilePath("/Parser/SimpleWorkbook.xlsx")

    val df = spark.read
      .format("com.elastacloud.spark.excel")
      .option("includeSheetName", value = true)
      .load(inputPath)

    df.count() should be(3)
    df.schema should equal(simpleWorkbookSchema.add(StructField("_SheetName", StringType, nullable = true)))

    df.select("_SheetName").head().getString(0) should be("Sheet1")
  }

  it should "work successfully using the short name" in {
    val inputPath = testFilePath("/Parser/SimpleWorkbook.xlsx")

    val df = spark.read.format("excel").load(inputPath)
    df.count() should be(3)
  }

  "Opening a password protected workbook" should "read all data if a valid password is provided" in {
    val inputPath = testFilePath("/Parser/PasswordProtectedWorkbook.xlsx")

    val df = spark.read
      .format("com.elastacloud.spark.excel")
      .option("workbookPassword", "password")
      .load(inputPath)

    df.count() should be(3)
  }

  "Opening a working in Excel 97-2003 format" should "open successfully with no additional configuration" in {
    val inputPath = testFilePath("/Parser/SimpleWorkbook.xls")

    val df = spark.read
      .format("com.elastacloud.spark.excel")
      .load(inputPath)

    df.count() should be(3)
    df.schema should equal(simpleWorkbookSchema)
  }

  "Opening a workbook with multiple sheets and headers" should "load with combined header and full data" in {
    val inputPath = testFilePath("/Parser/MultiSheetHeaderWorkbook.xlsx")

    import spark.implicits._

    val expectedDF = Seq(
      ("Pens", 100D, 200D, "2019"),
      ("Erasers", 28D, 37D, "2019"),
      ("Exercise books", 24D, 90D, "2019"),
      ("Pens", 100D, 200D, "2020"),
      ("Erasers", 28D, 37D, "2020"),
      ("Exercise books", 24D, 90D, "2020")
    ).toDF("Category", "Mar_Purchased", "Mar_Sold", "_SheetName")

    val df = spark.read
      .format("com.elastacloud.spark.excel")
      .option("sheetNamePattern", """\d{4}""")
      .option("headerRowCount", 2)
      .option("includeSheetName", value = true)
      .load(inputPath)
      .select($"Category", $"Mar_Purchased", $"Mar_Sold", $"_SheetName")

    val differences = expectedDF except df

    differences.count() should be(0)
  }

  "Reading a workbook with a specified schema" should "apply the defined schema" in {
    val inputPath = testFilePath("/Parser/SimpleWorkbook.xls")

    val dataSchema = StructType(Array(
      StructField("Col1", StringType, nullable = true),
      StructField("Col2", StringType, nullable = true),
      StructField("Col3", StringType, nullable = true)
    ))

    val df = spark.read
      .format("com.elastacloud.spark.excel")
      .schema(dataSchema)
      .load(inputPath)

    df.count() should be(3)
    df.schema should equal(dataSchema)
  }

  it should "apply the schema for basic non-native Excel data types" in {
    val inputPath = testFilePath("/Parser/CalculatedData.xlsx")

    val dataSchema = StructType(Array(
      StructField("Col_A", IntegerType, nullable = true),
      StructField("Col_B", DateType, nullable = true),
      StructField("Col_C", FloatType, nullable = true),
      StructField("Col_D", DateType, nullable = true),
      StructField("Col_E", DoubleType, nullable = true)
    ))

    val df = spark.read
      .format("com.elastacloud.spark.excel")
      .schema(dataSchema)
      .load(inputPath)

    df.count() should be(10)
    df.schema should equal(dataSchema)
  }

  "Reading a workbook as a SQL source" should "correctly load a simple workbook" in {
    val inputPath = testFilePath("/Parser/SimpleWorkbook.xls")

    val df = spark.sql(s"SELECT * FROM excel.`$inputPath`")

    df.count() should be(3)
    df.schema should equal(simpleWorkbookSchema)
  }

  "Reading a workbook" should "correctly read a file with spaces in the file name" in {
    val inputPath = testFilePath("/Parser/Spaces Workbook.xlsx")

    val df = spark.read
      .format("com.elastacloud.spark.excel")
      .load(inputPath.replace("%20", " "))

    df.count() should be(3)
  }

  "Reading an empty workbook" should "throw an exception" in {
    val inputPath = testFilePath("/Parser/Empty.xlsx")

    val error = intercept[ExcelParserException] {
      spark.read
        .format("excel")
        .load(inputPath.replace("%20", " "))
        .count()
    }

    error.getMessage should be("No data found on first row")
  }

  it should "return a single empty record if only headers exist" in {
    val inputPath = testFilePath("/Parser/NoData.xlsx")

    val dataSchema = StructType(Array(
      StructField("Col1", StringType, nullable = true),
      StructField("Col2", StringType, nullable = true),
      StructField("Col3", StringType, nullable = true),
      StructField("Col4", StringType, nullable = true)
    ))

    val df = spark.read
      .format("com.elastacloud.spark.excel")
      .schema(dataSchema)
      .load(inputPath)

    df.count() should be(1)
    df.schema should equal(dataSchema)
  }

  "Attempting to write to Excel" should "raise an error" in {
    import spark.implicits._

    val df = Seq(
      (1, 2, 3),
      (4, 5, 6)
    ).toDF

    val thrown = the[RuntimeException] thrownBy df.write
      .format("com.elastacloud.spark.excel")
      .save("example.xlsx")

    thrown.getMessage should include("Write is not supported")
  }
}
