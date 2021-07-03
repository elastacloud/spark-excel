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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File

class packageTests extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  private def testFilePath(relativePath: String): String = getClass.getResource(relativePath).getPath

  lazy val spark: SparkSession = SparkSession.builder()
    .appName("packageTests")
    .master("local[2]")
    .getOrCreate()

  override def afterAll(): Unit = {
    spark.close()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()

    super.afterAll()
  }

  "Reading excel with the package methods" should "retrieve the correct data when using a glob pattern" in {
    val inputFile = new File(testFilePath("/Parser/SimpleWorkbook.xls"))
    val globPath = s"${inputFile.getParent}${File.separatorChar}SimpleWorkbook.xls*"

    val df = spark.read.excel(globPath)
    df.count() should be(6)
  }

  it should "be able to read in multiple paths" in {
    val input1 = testFilePath("/Parser/SimpleWorkbook.xlsx")
    val input2 = testFilePath("/Parser/SimpleWorkbook.xls")

    val df = spark.read.excel(input1, input2)

    df.count() should be(6)
  }

  it should "apply options to all files being read" in {
    val input1 = testFilePath("/Parser/SimpleWorkbook.xlsx")
    val input2 = testFilePath("/Parser/SimpleWorkbook.xls")

    val df = spark.read
      .option("headerRowCount", 0)
      .option("cellAddress", "A2")
      .excel(input1, input2)

    val simpleWorkbookSchema = StructType(Array(
      StructField("col_0", StringType, nullable = true),
      StructField("col_1", DoubleType, nullable = true),
      StructField("col_2", StringType, nullable = true)
    ))

    df.schema should equal(simpleWorkbookSchema)
    df.count() should be(6)
  }
}
