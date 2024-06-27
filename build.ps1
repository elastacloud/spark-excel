<#
 .SYNOPSIS
    Tests and builds the Spark Excel data source source for difference versions of Spark

 .DESCRIPTION
    Runs tests and produces coverage information for all versions of Spark defined in the @versions parameter. Once
    this has completed an assembly is created for the spark version currently being processed.

    Once all versions of spark have been tested and assemblies built the jar files are copied to a top-level
    directory for easy access by the build system. In addition the coverage information for the most recent Spark
    version are also copied to a top level directory, again for easy access by the build system.

 .LINK
    https://www.elastacloud.com
#>

$versions = @("3.0.1", "3.0.2", "3.1.2", "3.2.1", "3.2.4", "3.3.0", "3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.5.0", "3.5.1")
$jarPath = "./target/jars"
$covPath = "./target/coverage"

Write-Host "Clearing existing artefacts" -ForegroundColor Green
if (Test-Path $jarPath)
{
    Remove-Item -Path $jarPath -Force -Recurse
}

if (Test-Path $covPath)
{
    Remove-Item -Path $covPath -Force -Recurse
}

New-Item -Path $jarPath -ItemType Directory
New-Item -Path $covPath -ItemType Directory

foreach ($version in $versions)
{
    Write-Host "Building for Spark version: $version" -ForegroundColor Green
    & sbt -DsparkVersion="$version" clean coverageOn test coverageReport coverageOff assembly
}

Write-Host "Copying jar files to $jarPath" -ForegroundColor Green
Get-ChildItem -Filter "spark-excel*.jar" -Path ./target -Recurse | Copy-Item -Destination $jarPath

Write-Host "Copying coverage information from most recent spark version to $covPath" -ForegroundColor Green
$maxVersion = ($versions | Measure-Object -Maximum).Maximum
Get-ChildItem -Path ".\target\spark-$maxVersion" -Recurse -Filter "scoverage-report" -Directory | Copy-Item -Destination .\target\coverage\ -Recurse
Get-ChildItem -Path ".\target\spark-$maxVersion" -Recurse -Filter "cobertura.xml" -File | Copy-Item -Destination .\target\coverage\
