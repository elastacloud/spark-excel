# Spark-Excel

[![Spark Excel CI](https://github.com/elastacloud/spark-excel/actions/workflows/spark.yml/badge.svg)](https://github.com/elastacloud/spark-excel/actions/workflows/spark.yml)
[![codecov](https://codecov.io/gh/elastacloud/spark-excel/branch/main/graph/badge.svg?token=M6313GUBPV)](https://codecov.io/gh/elastacloud/spark-excel)

A Spark data source for reading Microsoft Excel workbooks. Initially started to "scratch and itch" and to learn how to
write data sources using the
Spark [DataSourceV2](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/sources/v2/DataSourceV2.html)
APIs. This is based on the [Apache POI](https://poi.apache.org/) library which provides the means to read Excel files.

## Important

This repository makes use of [Git-LFS](https://git-lfs.github.com/) to track the Excel files for unit tests. This allows
for larger files to be uploaded as part of the tests without bloating the repository. If cloning the code you will need
to ensure you have Git-LFS installed and available.

## Features

- Handling Excel 97-2003, 2010, and OOXML files (thanks to Apache POI)
- Multi-line headers
- Reading from multiple worksheets given a name pattern
- Glob pattern support for reading multiple files
- Outputting the sheet name in the result set (set via an option)
- Schema inference
- Cleans column names on read to remove invalid characters which can cause issues in Spark (spaces, periods etc...)
- Handle merged cells (repeats data to all cells in the merged region)
- Formula evaluation (for those supported by Apache POI)
- Works in Scala, PySpark, and Spark SQL

## Usage

Standard usage of the library works as follows:

```scala
// Scala
val df = spark.read
  .format("com.elastacloud.spark.excel")
  .option("cellAddress", "A1")
  .load("/path/to/my_file.xlsx")
```

```python
# Python
df = spark.read
    .format("com.elastacloud.spark.excel")
    .option("cellAddress", "A1")
    .load("/path/to/my_file.xlsx")
```

A short name has been provided for convenience, as well as convenience method (Scala only currently).

```scala
val df = spark.read
  .format("excel")
  .load("/path/to/my_file.xlsx")
```

```scala
val df = spark.read
  .excel("/path/to/my_file.xlsx")
```

All of these methods accept glob patterns and multiple path values.

```scala
// Providing multiple paths
val multiFileDF = spark.read
  .format("com.elastacloud.spark.excel")
  .excel("/path/to/my_file.xlsx", "/path/to/my_second_file.xlsx")

// Using glob patterns
val globFileDF = spark.read
  .format("com.elastacloud.spark.excel")
  .excel("/landing/20*/*.xlsx")
```

## Options

The library supports the following options:

Option           | Type    | Default | Description
---------------- | ------- | ------- | -----------
cellAddress      | String  | A1      | Location of the first cell of the table (including header)
headerRowCount   | Int     | 1       | Number of rows which make up the header. If no header is available then set this value to 0 (zero)
includeSheetName | Boolean | False   | Includes the name of the worksheet the data has come from when set to true. Uses the column `_SheetName`
workbookPassword | String  | _Empty_ | Password required to open Excel workbook
sheetNamePattern | String  | _Empty_ | Regular expression to use to match worksheet names
maxRowCount      | Int     | 1000    | Number of records to read to infer the schema. If set to 0 (zero) then all available rows will be read

```scala
val df = spark.read
  .format("com.elastacloud.spark.excel")
  .option("cellAddress", "C3")                 // The first line of the table starts at cell C3
  .option("headerRowCount", 2)                 // The first 2 lines of the table make up the header row
  .option("includeSheetName", value = true)    // Include the sheet name(s) the data has come from
  .option("workbookPassword", "AP@55w0rd")     // Use this password to open the workbook with
  .option("sheetNamePattern", """Sheet[13]""") // Read data from all sheets matching this pattern (e.g. Sheet1 and Sheet3)
  .option("maxRowCount", 10)                    // Read only the first 10 records to determine the schema of the data
  .load("/path/to/file.xlsx")
```

### Multi row headers

When reading multiple rows as a header the parser attempts to combine the content of all cells in that column (including
merged regions) to create an overall name. For example.

```text
+--------+--------------------+
|        | 2021               |
+--------+--------------------+
| Id     | Target   | Actual  |
+--------+--------------------+
| 1      | 100      | 98      |
| 2      | 200      | 230     |
+--------+----------+---------+
```

In this case the resulting DataFrame would look as follows.

```text
+--------+--------------+-------------+
| Id     | 2021_Target  | 2021_Actual |
+--------+----------------------------+
| 1      | 100          | 98          |
| 2      | 200          | 230         |
+--------+--------------+-------------+
```

### Sheet name patterns

Often when reading in data from Excel we'll find that there are multiple sheets containing the same shape data, but have
been split apart to add context, such as the month, year, department name and so on. To avoid having to add multiple
reads of the same file to access difference sheets, the data source provides the `sheetNamePattern` option, which takes
a regular expression. This pattern evaluates for any sheets matching the pattern which are then included in the result
set.

If, for example, you wanted to include sheet names for the next 3 years (assuming the year is currently 2021) then you
could provide a pattern of `202[234]`. This would ignore any sheets for other years, summary sheets etc...

Often when using this option it is useful to include the sheet name so that the original context is not lost, in the same
way you might want to include the source file name when reading from multiple files. And so the code might read like.

```scala
val df = spark.read
  .format("com.elastacloud.spark.excel")
  .option("sheetNamePattern", "202[234]")
  .option("includeSheetName", "true")
  .load("/path/to/my_file.xlsx")
```
