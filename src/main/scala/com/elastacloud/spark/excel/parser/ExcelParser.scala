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
import com.elastacloud.spark.excel.parser.ExcelParser.providersAdded
import org.apache.poi.hssf.usermodel.HSSFWorkbookFactory
import org.apache.poi.openxml4j.util.{ZipInputStreamZipEntrySource, ZipSecureFile}
import org.apache.poi.ss.usermodel._
import org.apache.poi.ss.util.CellAddress
import org.apache.poi.xssf.usermodel.XSSFWorkbookFactory
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.io.InputStream
import java.sql.{Date, Timestamp}
import java.time.format.DateTimeFormatter
import scala.collection.JavaConverters._

private[excel] class ExcelParser(inputStream: InputStream, options: ExcelParserOptions, schema: Option[StructType] = None, readSchema: Option[StructType] = None) {
  private final val sheetFieldName: String = "_SheetName"

  /**
   * Instance of the workbook, opened using the password if provided
   */
  private val workBook: Workbook = {
    // Make sure that the correct providers are added
    val types = Seq[WorkbookProvider](new HSSFWorkbookFactory, new XSSFWorkbookFactory)

    // Use synchronized to prevent concurrency issues in tests
    this.synchronized {
      // Use the flag so that this is only done one time
      if (!providersAdded) {
        for (elem <- types) {
          WorkbookFactory.removeProvider(elem.getClass)
          WorkbookFactory.addProvider(elem)
        }
        providersAdded = true
      }
    }

    ZipSecureFile.setMinInflateRatio(0)
    ZipInputStreamZipEntrySource.setThresholdBytesForTempFiles(options.thresholdBytesForTempFiles)

    options.workbookPassword match {
      case Some(password) => WorkbookFactory.create(inputStream, password)
      case _ => WorkbookFactory.create(inputStream)
    }
  }

  /**
   * An instance of the formula evaluate for the current workbook
   */
  private val formulaEvaluator = if (options.evaluateFormulae) {
    Some(workBook.getCreationHelper.createFormulaEvaluator())
  } else {
    None
  }

  /**
   * The indexes of the worksheets which match the sheet name regular expression pattern
   */
  private[excel] val sheetIndexes: Seq[Int] = if (options.sheetNamePattern.trim.isEmpty) {
    Seq(0)
  } else {
    0.until(workBook.getNumberOfSheets).filter(i => workBook.getSheetAt(i).getSheetName.matches(options.sheetNamePattern))
  }

  if (sheetIndexes.isEmpty) {
    throw new ExcelParserException("No worksheets found matching user defined pattern")
  }

  /**
   * The first cell of the table to read data from
   */
  private val firstCellAddress = new CellAddress(options.cellAddress)

  /**
   * Defines a pattern to identify characters which are invalid in column names for Spark
   */
  private val invalidFieldNameChars = """[+\\._, \n]""".r

  /**
   * The schema of the Excel data
   */
  private var excelSchema: Option[StructType] = schema

  /**
   * Reads the schema from the Excel source based on the first sheet in the set which matches the users requirements
   *
   * @return the schema as a [[StructType]] value
   */
  def readDataSchema(): StructType = excelSchema match {
    case None =>
      excelSchema = Some(inferSchema())
      excelSchema.get
    case _ => excelSchema.get
  }

  /**
   * Reads the schema required for the output from Excel
   *
   * @return the schema as a [[StructType]] value
   */
  private def readOutputSchema(): StructType = readSchema match {
    case None => readDataSchema()
    case Some(s) => s
  }

  /**
   * Gets an iterator to read data from all matching sheets in the workbook
   *
   * @return a workbook data iterator
   */
  def getDataIterator: WorkbookDataIterator = {
    new WorkbookDataIterator
  }

  /**
   * Provides methods for iterating over data in a workbook
   */
  final class WorkbookDataIterator extends Iterator[Seq[Any]] {
    private val sheetIterator = getSheetIterator
    private var currentSheet: Sheet = _
    private var currentSheetName: UTF8String = _
    private var firstDataRow: Int = _
    private var lastDataRow: Int = _
    private var currentDataRow: Int = _
    private val lastColumnIndex = firstCellAddress.getColumn + readDataSchema().length
    private val outputFields = readOutputSchema().map(f => f.name)

    // Check if the schema match column name exists
    if (options.schemaMatchColumnName != null && !outputFields.exists(_.equalsIgnoreCase(options.schemaMatchColumnName))) {
      throw new ExcelParserException("The specified schema match column does not exist within the schema.")
    }

    // Check that the data type of the schema match column is correct
    if (options.schemaMatchColumnName != null) {
      val schemaMatchField = readOutputSchema().filter(_.name.equalsIgnoreCase(options.schemaMatchColumnName)).head
      if (schemaMatchField.dataType != BooleanType) {
        throw new ExcelParserException("The specified schema match column is not defined as a boolean type.")
      }
    }

    /**
     * Identifies if the iterator contains more values
     *
     * @return true if there is more data available, otherwise false
     */
    override def hasNext: Boolean = {
      if (currentSheet == null) {
        loadNextSheet()
        true
      } else if (currentDataRow <= lastDataRow) {
        true
      } else if (sheetIterator.hasNext) {
        loadNextSheet()
        true
      } else {
        false
      }
    }

    /**
     * Check the output of the value against the output schema, returning am [[Option]] with None if the value is
     * not expected
     *
     * @param value     the value attempting to be returned
     * @param fieldName the name of the field being evaluated
     * @return an [[Option]] containing the value
     */
    private def checkOutput(value: Any, fieldName: String): Option[Any] = {
      if (outputFields.contains(fieldName)) {
        Some(value)
      } else {
        None
      }
    }

    /**
     * Returns the next set of data from the iterator
     *
     * @return a sequence of items representing the row of data
     */
    override def next(): Seq[Any] = {
      val currentRow = currentSheet.getRow(currentDataRow)
      var rowMatchesSchema = true
      // If the data row does not exist then return a row of null values
      val rowData = if (currentRow == null) {
        firstCellAddress.getColumn.until(lastColumnIndex).zipWithIndex.map { case (_, i) =>
          if (readDataSchema()(i).name == sheetFieldName && options.includeSheetName) {
            checkOutput(currentSheetName, readDataSchema()(i).name)
          } else if (options.schemaMatchColumnName != null && readDataSchema()(i).name == options.schemaMatchColumnName) {
            checkOutput(false, readDataSchema()(i).name)
          } else {
            checkOutput(null, readDataSchema()(i).name)
          }
        }
      } else {
        firstCellAddress.getColumn.until(lastColumnIndex).zipWithIndex.map { case (columnIndex, i) =>
          if (readDataSchema()(i).name == sheetFieldName && options.includeSheetName) {
            checkOutput(currentSheetName, readDataSchema()(i).name)
          } else if (options.schemaMatchColumnName != null && readDataSchema()(i).name == options.schemaMatchColumnName) {
            checkOutput(rowMatchesSchema, readDataSchema()(i).name)
          } else {
            val cell = currentRow.getCell(columnIndex, Row.MissingCellPolicy.RETURN_NULL_AND_BLANK)
            val cellValue = if (cell == null) {
              (null, readDataSchema()(i).nullable)
            } else {
              val currentCell = getMergedCell(cell)
              val targetType = readDataSchema()(i).dataType
              getCellValue(currentCell, targetType, readDataSchema()(i).nullable)
            }

            if (!cellValue._2) {
              rowMatchesSchema = false
            }

            checkOutput(cellValue._1, readDataSchema()(i).name)
          }
        }
      }

      currentDataRow += 1
      rowData.flatten
    }

    /**
     * Helper method for loading the next sheet in the workbook and resetting state
     */
    private def loadNextSheet(): Unit = {
      currentSheet = sheetIterator.next()
      currentSheetName = UTF8String.fromString(currentSheet.getSheetName)
      firstDataRow = firstCellAddress.getRow + options.headerRowCount
      lastDataRow = currentSheet.getLastRowNum
      currentDataRow = firstDataRow
    }

    /**
     * Gets the cell value from the worksheet and formats to match the target type
     *
     * @param cell           the cell to copy data from
     * @param targetType     the type of the spark field where the data will be parsed to
     * @param targetNullable indicates if the field in the target schema allows null values
     * @return the cell value targeting the destination type, and a boolean flag indicating if the cell value matches
     *         the target schema
     */
    private def getCellValue(cell: Cell, targetType: DataType, targetNullable: Boolean): (Any, Boolean) = {
      val currentCell = getMergedCell(cell)

      val invalidCellTypes = Seq(CellType._NONE, CellType.BLANK, CellType.ERROR)

      if (currentCell == null || invalidCellTypes.contains(currentCell.getCellType)) {
        return (null, targetNullable)
      }

      val evaluatedFormulaCell = formulaEvaluator match {
        case Some(evaluator) => Some(evaluator.evaluate(currentCell))
        case None => None
      }

      val cellType = evaluatedFormulaCell match {
        case Some(evaluatedCell) => evaluatedCell.getCellType
        case None => currentCell.getCellType
      }

      cellType match {
        case CellType._NONE | CellType.BLANK | CellType.ERROR => (null, targetNullable)
        case CellType.BOOLEAN => targetType match {
          case _: StringType =>
            evaluatedFormulaCell match {
              case Some(evaluatedCell) => (UTF8String.fromString(evaluatedCell.getBooleanValue.toString), true)
              case None => (UTF8String.fromString(currentCell.getBooleanCellValue.toString), true)
            }
          case _: BooleanType =>
            evaluatedFormulaCell match {
              case Some(evaluatedCell) => (evaluatedCell.getBooleanValue, true)
              case None => (currentCell.getBooleanCellValue, true)
            }
          case _ => (null, false)
        }
        case CellType.NUMERIC => targetType match {
          case _: StringType => if (DateUtil.isCellDateFormatted(currentCell)) {
            evaluatedFormulaCell match {
              case Some(evaluatedCell) => (UTF8String.fromString(DateUtil.getLocalDateTime(evaluatedCell.getNumberValue).format(DateTimeFormatter.ISO_DATE_TIME)), true)
              case None => (UTF8String.fromString(DateUtil.getLocalDateTime(currentCell.getNumericCellValue).format(DateTimeFormatter.ISO_DATE_TIME)), true)
            }
          } else {
            evaluatedFormulaCell match {
              case Some(evaluatedCell) => (UTF8String.fromString(evaluatedCell.getNumberValue.toString), true)
              case None => (UTF8String.fromString(currentCell.getNumericCellValue.toString), true)
            }
          }
          case _: TimestampType if DateUtil.isCellDateFormatted(currentCell) =>
            val ts = evaluatedFormulaCell match {
              case Some(evaluatedCell) => Timestamp.valueOf(DateUtil.getLocalDateTime(evaluatedCell.getNumberValue))
              case None => Timestamp.valueOf(DateUtil.getLocalDateTime(currentCell.getNumericCellValue))
            }
            (DateTimeUtils.fromJavaTimestamp(ts), true)
          case _: DateType if DateUtil.isCellDateFormatted(currentCell) =>
            val ts = evaluatedFormulaCell match {
              case Some(evaluatedCell) => Timestamp.valueOf(DateUtil.getLocalDateTime(evaluatedCell.getNumberValue))
              case None => Timestamp.valueOf(DateUtil.getLocalDateTime(currentCell.getNumericCellValue))
            }
            (DateTimeUtils.fromJavaDate(Date.valueOf(ts.toLocalDateTime.toLocalDate)), true)
          case _: IntegerType => evaluatedFormulaCell match {
            case Some(evaluatedCell) => (evaluatedCell.getNumberValue.toInt, true)
            case None => (currentCell.getNumericCellValue.toInt, true)
          }
          case _: LongType => evaluatedFormulaCell match {
            case Some(evaluatedCell) => (evaluatedCell.getNumberValue.toLong, true)
            case None => (currentCell.getNumericCellValue.toLong, true)
          }
          case _: FloatType => evaluatedFormulaCell match {
            case Some(evaluatedCell) => (evaluatedCell.getNumberValue.toFloat, true)
            case None => (currentCell.getNumericCellValue.toFloat, true)
          }
          case _: DoubleType => evaluatedFormulaCell match {
            case Some(evaluatedCell) => (evaluatedCell.getNumberValue, true)
            case None => (currentCell.getNumericCellValue, true)
          }
          case _ => (null, false)
        }
        case CellType.STRING => targetType match {
          case _: StringType =>
            val cellStringValue = evaluatedFormulaCell match {
              case Some(evaluatedCell) => UTF8String.fromString(evaluatedCell.getStringValue)
              case None => UTF8String.fromString(currentCell.getStringCellValue)
            }
            options.nulLValue match {
              case Some(nullValue) if cellStringValue.toString.equalsIgnoreCase(nullValue) => (null, true)
              case _ => (cellStringValue, true)
            }
          case _ => (null, false)
        }
        case _ => evaluatedFormulaCell match {
          case Some(evaluatedCell) => (UTF8String.fromString(evaluatedCell.toString), true)
          case None => (UTF8String.fromString(currentCell.toString), true)
        }
      }
    }
  }

  /**
   * Get an iterator on the sheets matching the user sheet name pattern
   *
   * @return an iterator of sheet index values
   */
  private def getSheetIterator: Iterator[Sheet] = {
    sheetIndexes.map(i => workBook.getSheetAt(i)).toIterator
  }

  /**
   * Gets the cell from the worksheet which holds the data for a merged cell region
   *
   * @param cell a worksheet [[Cell]]
   * @return the worksheet [[Cell]] containing the data for the merged region
   */
  private def getMergedCell(cell: Cell): Cell = {
    val sheet = cell.getSheet
    val mergedRegions = sheet.getMergedRegions.asScala
    val mergedRange = mergedRegions.exists(p => p.isInRange(cell))

    if (mergedRange) {
      mergedRegions.filter(p => p.isInRange(cell)).map(p => sheet.getRow(p.getFirstRow).getCell(p.getFirstColumn)).head
    } else {
      cell
    }
  }

  /**
   * Scans the first matching worksheet in the workbook to infer the schema. Where a column has multiple types
   * the inferred type will default to [[StringType]]
   *
   * @return a [[StructType]] representing the schema for the Excel source
   */
  private def inferSchema(): StructType = {
    val sheet = workBook.getSheetAt(sheetIndexes.head)
    val firstRow = sheet.getRow(firstCellAddress.getRow)

    if (firstRow == null) throw new ExcelParserException("No data found on first row")

    val firstColumnIndex = firstCellAddress.getColumn
    val lastColumnIndex = firstRow.getLastCellNum.toInt

    var firstDataRow = firstRow

    // Get the field names for the workbook. If the header option is defined then take the names from the column headers in
    // the sheet. If not then generate the column names as "col_<index>" from the zero-based index
    val fieldNames = if (options.headerRowCount > 0) {
      val lastHeaderRow = firstCellAddress.getRow + options.headerRowCount - 1
      // Set the first data row as the row after the header row
      firstDataRow = sheet.getRow(firstCellAddress.getRow + options.headerRowCount)
      firstColumnIndex.until(lastColumnIndex).zipWithIndex.map { case (colIndex, i) =>
        // Get the current cell value (checking if it's part of a merged region). If the cell is null/blank then the header
        // is set to "col_<index>" from the zero based index, otherwise the field name is cleaned
        val headerContent = firstRow.getRowNum.to(lastHeaderRow).map { rowIndex =>
          val currentHeaderCell = sheet.getRow(rowIndex).getCell(colIndex, Row.MissingCellPolicy.RETURN_NULL_AND_BLANK)
          if (currentHeaderCell == null) "" else getMergedCell(currentHeaderCell).toString
        }.distinct.mkString(" ")

        val cleanedName = if (headerContent.trim.isEmpty) s"col_$i" else {
          invalidFieldNameChars
            .replaceAllIn(headerContent.trim, "_")
            .replaceAll("""_+""", "_")
            .stripSuffix("_")
        }
        cleanedName
      }
    } else {
      firstColumnIndex.to(lastColumnIndex).zipWithIndex.map { case (_, i) => s"col_$i" }
    }

    // Determine the last data row, this is either the last row of data, or the maximum number of rows defined by the user
    val lastRowNum = options.maxRowCount match {
      case rowNum if rowNum != 0 && rowNum + firstDataRow.getRowNum <= sheet.getLastRowNum => rowNum + firstDataRow.getRowNum
      case _ => sheet.getLastRowNum
    }

    // Get the field structure for data in the workbook
    var fields = firstColumnIndex.until(lastColumnIndex).zipWithIndex.map { case (colIndex, i) =>
      // Get the collection of types for the current column across the rows used for inferring the schema
      val colTypes = firstDataRow.getRowNum.until(lastRowNum).flatMap(rowIndex => {
        // Get the current cell (or cell containing data for part of a merged region), the determine the Spark DataType
        // for the cell
        val currentCell = sheet.getRow(rowIndex).getCell(colIndex, Row.MissingCellPolicy.RETURN_NULL_AND_BLANK)
        val fieldType: Option[DataType] = if (currentCell == null || currentCell.getCellType == CellType.BLANK) None else {
          val cellType = formulaEvaluator match {
            case Some(evaluator) => evaluator.evaluate(currentCell).getCellType
            case None => currentCell.getCellType
          }

          cellType match {
            case CellType._NONE | CellType.BLANK | CellType.ERROR => None
            case CellType.BOOLEAN => Some(BooleanType)
            case CellType.NUMERIC => if (DateUtil.isCellDateFormatted(currentCell)) Some(TimestampType) else Some(DoubleType)
            case _ => Some(StringType)
          }
        }
        fieldType
      })

      // If all of the cells in the inference set are of the same type, then use this as the schema type, otherwise
      // default to data as a string
      if (colTypes.distinct.length == 1) {
        StructField(fieldNames(i), colTypes.head, nullable = true)
      } else {
        StructField(fieldNames(i), StringType, nullable = true)
      }
    }

    if (options.includeSheetName) {
      fields = fields :+ StructField(sheetFieldName, StringType, nullable = false)
    }

    if (options.schemaMatchColumnName != null) {
      if (fields.exists(f => f.name.equalsIgnoreCase(options.schemaMatchColumnName))) {
        throw new ExcelParserException("The specified schema match column conflicts with a column of the same name in the data set.")
      }
      fields = fields :+ StructField(options.schemaMatchColumnName, BooleanType, nullable = false)
    }

    StructType(fields)
  }

  /**
   * Closes the parser and releases any resources (excluding the [[InputStream]] used to create the instance)
   */
  def close(): Unit = {
    workBook.close()
  }
}

object ExcelParser {
  /**
   * Flag to determine if the Excel workbook factory providers have been added. This is typically not required
   * in deployed scenarios, but the unit tests become unstable without it and the logic in the class. And we
   * all love working unit tests.
   */
  private var providersAdded = false

  /**
   * Read the schema from an Excel workbook based on user defined [[ExcelParserOptions]]
   *
   * @param inputStream the [[InputStream]] for the Excel input
   * @param options     the user defined options for reading the Excel input
   * @return a [[StructType]] representing the schema for the Excel input
   */
  def schemaFromWorkbook(inputStream: InputStream, options: ExcelParserOptions): StructType = {
    val parser = new ExcelParser(inputStream, options)
    try {
      parser.readDataSchema()
    } finally {
      parser.close()
    }
  }
}
