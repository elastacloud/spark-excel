package com.elastacloud.spark.excel

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters.mapAsJavaMapConverter

class ExcelParserOptionsTests extends AnyFlatSpec with Matchers {
  behavior of "Creating a default instance"

  it should "use default values" in {
    val options = new ExcelParserOptions()

    options.workbookPassword should be(None)
    options.sheetNamePattern shouldBe empty
    options.cellAddress should be("A1")
    options.headerRowCount should be(1)
    options.maxRowCount should be(1000)
    options.includeSheetName should be(false)
  }

  behavior of "Creating from a case insensitive map"

  it should "use default values for an empty map" in {
    val input = new CaseInsensitiveStringMap(Map[String, String]().asJava)

    val options = ExcelParserOptions.from(input)

    options.workbookPassword should be(None)
    options.sheetNamePattern shouldBe empty
    options.cellAddress should be("A1")
    options.headerRowCount should be(1)
    options.maxRowCount should be(1000)
    options.includeSheetName should be(false)
  }

  it should "extract values from the map" in {
    val input = new CaseInsensitiveStringMap(Map[String, String](
      "workbookPassword" -> "abc123",
      "sheetNamePattern" -> "Sheet[12]",
      "cellAddress" -> "B3",
      "headerRowCount" -> "12",
      "maxRowCount" -> "2000",
      "includeSheetName" -> "true"
    ).asJava)

    val options = ExcelParserOptions.from(input)

    options.workbookPassword should be(Some("abc123"))
    options.sheetNamePattern should be("Sheet[12]")
    options.cellAddress should be("B3")
    options.headerRowCount should be(12)
    options.maxRowCount should be(2000)
    options.includeSheetName should be(true)
  }

  it should "provide useful error information if options are slightly mis-spelt" in {
    val input = new CaseInsensitiveStringMap(Map[String, String](
      "workbookPasword" -> "abc123",
      "sheetNamPatten" -> "Sheet[12]",
      "cellAdres" -> "B3",
      "hdrRowCount" -> "12",
      "maxRowCont" -> "2000",
      "includShetNam" -> "true"
    ).asJava)

    val exception = the[ExcelParserOptionsException] thrownBy ExcelParserOptions.from(input)

    exception.getMessage.contains("Invalid option 'workbookPasword' did you mean 'workbookPassword'?")
    exception.getMessage.contains("Invalid option 'sheetNamPatten' did you mean 'sheetNamePattern'?")
    exception.getMessage.contains("Invalid option 'cellAdres' did you mean 'cellAddress'?")
    exception.getMessage.contains("Invalid option 'hdrRowCount' did you mean 'headerRowCount'?")
    exception.getMessage.contains("Invalid option 'maxRowCont' did you mean 'maxRowCount'?")
    exception.getMessage.contains("Invalid option 'includShetNam' did you mean 'includeSheetName'?")
  }

  it should "ignore options which are invalid and not close in spelling to valid options" in {
    val input = new CaseInsensitiveStringMap(Map[String, String](
      "pwd" -> "abc123"
    ).asJava)

    val options = ExcelParserOptions.from(input)

    options.workbookPassword should be(None)
  }

  behavior of "Creating from a string map"

  it should "use default values for an empty map" in {
    val input = Map[String, String]()

    val options = ExcelParserOptions.from(input)

    options.workbookPassword should be(None)
    options.sheetNamePattern shouldBe empty
    options.cellAddress should be("A1")
    options.headerRowCount should be(1)
    options.maxRowCount should be(1000)
    options.includeSheetName should be(false)
  }

  it should "extract values from the map" in {
    val input = Map[String, String](
      "workbookPassword" -> "abc123",
      "sheetNamePattern" -> "Sheet[12]",
      "cellAddress" -> "B3",
      "headerRowCount" -> "12",
      "maxRowCount" -> "2000",
      "includeSheetName" -> "true"
    )

    val options = ExcelParserOptions.from(input)

    options.workbookPassword should be(Some("abc123"))
    options.sheetNamePattern should be("Sheet[12]")
    options.cellAddress should be("B3")
    options.headerRowCount should be(12)
    options.maxRowCount should be(2000)
    options.includeSheetName should be(true)
  }
}
