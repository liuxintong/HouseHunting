
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('All', 'Download', 'Extract', 'Reload', 'SqlGen')]
    [string]$Action
)

$Script:ErrorActionPreference = 'Stop'

$app7zPath = 'C:\Program Files\7-Zip\7z.exe'
$appMySqlPath = 'C:\Program Files\MariaDB 10.6\bin\mysql.exe'

$timestamp = (Get-Date).ToString('yyyyMMdd')
$dataFolder = Join-Path $PSScriptRoot 'data'
$dailyFolder = Join-Path $dataFolder 'daily'
$todayFolder = Join-Path $dailyFolder $timestamp
$latestFolder = Join-Path $dataFolder 'latest'
$logFolder = Join-Path $dataFolder 'logs'
$logFile = Join-Path $logFolder 'RedfinDataHandler.log'
$sqlFolder = Join-Path $PSScriptRoot 'sql'

New-Item -Path $dataFolder -ItemType Directory -Force | Write-Verbose
New-Item -Path $dailyFolder -ItemType Directory -Force | Write-Verbose
New-Item -Path $todayFolder -ItemType Directory -Force | Write-Verbose
New-Item -Path $latestFolder -ItemType Directory -Force | Write-Verbose
New-Item -Path $logFolder -ItemType Directory -Force | Write-Verbose
New-Item -Path $sqlFolder -ItemType Directory -Force | Write-Verbose

$filesToDownload = @(
    # Redfin data source: https://www.redfin.com/news/data-center/
    'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_covid19/weekly_housing_market_data_most_recent.tsv000',
    'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/us_national_market_tracker.tsv000.gz',
    'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/redfin_metro_market_tracker.tsv000.gz',
    'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/state_market_tracker.tsv000.gz',
    'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/county_market_tracker.tsv000.gz',
    'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/city_market_tracker.tsv000.gz',
    'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/zip_code_market_tracker.tsv000.gz'
    'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/neighborhood_market_tracker.tsv000.gz'
)

function Invoke-DataDownload
{
    [CmdletBinding()]
    param()

    "[$(Get-Date)] [$timestamp] start downloading $($filesToDownload.Count) files" | Add-Content $logFile

    foreach ($file in $filesToDownload)
    {
        $startTime = Get-Date
        $fileName = Split-Path -Path $file -Leaf
        "[$startTime] [$fileName] downloading '$file'" | Add-Content $logFile

        $filePath = Join-Path $todayFolder $fileName
        Invoke-WebRequest -Uri $file -OutFile $filePath | Add-Content $logFile

        $md5 = (Get-FileHash -Path $filePath -Algorithm MD5).Hash
        $filePathWithMd5 = Join-Path $todayFolder "$md5.$fileName"
        Move-Item -Path $filePath -Destination $filePathWithMd5 | Write-Verbose

        $endTime = Get-Date
        "[$endTime] [$fileName] downloaded to '$filePathWithMd5' in $(($endTime - $startTime).TotalSeconds) seconds" | Add-Content $logFile
    }

    "[$(Get-Date)] [$timestamp] finish downloading $($filesToDownload.Count) files" | Add-Content $logFile
}

function Invoke-DataExtract
{
    [CmdletBinding()]
    param()

    "[$(Get-Date)] [$timestamp] start extracting $($filesToDownload.Count) files" | Add-Content $logFile

    foreach ($file in $filesToDownload)
    {
        $fileName = Split-Path -Path $file -Leaf

        $latestFile = Get-ChildItem -Path $dailyFolder -Filter "*.$fileName" -Recurse | Sort-Object Attributes.Directory.Name -Descending | Select-Object -First 1
        $sourcePath = $latestFile.FullName
        $destinationPath = Join-Path $latestFolder $fileName.TrimEnd('.gz')

        if ($fileName -like '*.gz')
        {
            "[$(Get-Date)] [$fileName] extracting latest file from '$sourcePath'" | Add-Content $logFile
            & $app7zPath x -so -y $sourcePath > $destinationPath
            
            if ($LASTEXITCODE)
            {
                $msg = "Failed to extract file from $sourcePath to $destinationPath"
                $msg | Add-Content $logFile
                throw $msg
            }
        }
        else
        {
            "[$(Get-Date)] [$fileName] copying latest file from '$sourcePath'" | Add-Content $logFile
            Copy-Item -Path $sourcepath -Destination $destinationPath | Add-Content $logFile
        }
    }

    "[$(Get-Date)] [$timestamp] finish extracting $($filesToDownload.Count) files" | Add-Content $logFile
}

function Invoke-DataReload {
    [CmdletBinding()]
    param()

    "[$(Get-Date)] [$timestamp] start reloading $($filesToDownload.Count) files" | Add-Content $logFile

    foreach ($file in $filesToDownload)
    {
        $fileName = Split-Path -Path $file -Leaf
        $dataName = $fileName -replace '.gz|.tsv000'
        $queryPath = Join-Path $sqlFolder "$dataName.sql"

        $query = Get-Content -Path $queryPath
        $dataPath = Join-Path $latestFolder "$dataName.tsv000"
        $query = $query.Replace("$dataName.tsv", $($dataPath -replace '\\', '\\'))
        
        $credPath = Join-Path $dataFolder 'config' 'mariadb.json'
        $cred = Get-Content $credPath | ConvertFrom-Json

        $startTime = Get-Date
        "[$startTime] [$dataName] executing query '$queryPath'" | Add-Content $logFile
        $query | & $appMySqlPath "-h$($cred.HostName)" "-u$($cred.UserName)" "-p$($cred.Password)" housing 2>&1 | Add-Content $logFile

        $endTime = Get-Date
        "[$endTime] [$dataName] data loaded in $(($endTime - $startTime).TotalSeconds) seconds" | Add-Content $logFile

        if ($LASTEXITCODE)
        {
            $msg = "Failed to execute query $queryPath"
            $msg | Add-Content $logFile
            throw $msg
        }
    }

    "[$(Get-Date)] [$timestamp] finish reloading $($filesToDownload.Count) files" | Add-Content $logFile
}

function Invoke-SqlGenHelper
{
    [CmdletBinding()]
    param()

    $files = Get-ChildItem -Path $latestFolder -Filter '*.tsv000'
    Write-Warning "Generating SQL schema files for $($files.Length) tsv files"
    Write-Warning "Files under '$sqlFolder' will be overwritten, continue? [y/N]"

    if ((Read-Host) -ne 'y')
    {
        return
    }

    foreach ($file in $files)
    {
        $tableName = $file.Name -replace '.tsv000$'
        $genFilePath = Join-Path $sqlFolder "$tableName.sql"
        Write-Warning "Generating sql file '$genFilePath'"

        $firstLine = Get-Content -Path $file.FullName -TotalCount 1
        Write-Host "First line: $firstLine"
        $columns = $firstLine -split '\t'
        $types = @{}

        foreach ($col in $columns)
        {
            if ($col -match 'period_begin|period_end')
            {
                $types[$col] = 'DATE'
            }
            elseif ($col -match 'last_updated')
            {
                $types[$col] = 'DATETIME'
            }
            elseif ($col -match 'period_duration|region_type_id|table_id|property_type_id|parent_metro_region_metro_code')
            {
                $types[$col] = 'INT'
            }
            elseif ($col -match 'region_type|is_seasonally_adjusted|region|city|state|state_code|property_type|parent_metro_region|duration')
            {
                $types[$col] = 'VARCHAR(32)'
            }
            else
            {
                $types[$col] = 'DECIMAL(32, 24)'
            }
        }

        @"
/* the file name $tableName.tsv must be replaced with a real absolute file path */

USE ``housing``;

CREATE OR REPLACE TABLE ``housing``.``$tableName`` (
$(
    ($columns | ForEach-Object { "    $_ $($types[$_])" }) -join ",`n"
)
);

SELECT * FROM $tableName LIMIT 10;

LOAD DATA LOW_PRIORITY LOCAL INFILE '$tableName.tsv'
INTO TABLE ``housing``.``$tableName``
CHARACTER SET ASCII
FIELDS TERMINATED BY '\t'
OPTIONALLY ENCLOSED BY '"'
ESCAPED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(
$(
    ($columns | ForEach-Object { "    @$_" }) -join ",`n"
)
)
SET
$(
    ($columns | ForEach-Object { "``$_`` = NULLIF(@$_, '')" }) -join ",`n"
);

$(
    ($columns | Where-Object { $($types[$_]) -notmatch '^DECIMAL' } | ForEach-Object { "CREATE OR REPLACE INDEX ``$_`` ON ``housing``.``$tableName`` (``$_``);" }) -join "`n"
)
"@ | Set-Content -Path $genFilePath
    }

    Write-Warning "Generated SQL $($files.Length) schema files"
}

switch ($Action)
{
    'All' { Invoke-DataDownload; Invoke-DataExtract; Invoke-DataReload; }
    'Download' { Invoke-DataDownload }
    'Extract' { Invoke-DataExtract }
    'Reload' { Invoke-DataReload }
    'SqlGen' { Invoke-SqlGenHelper }
    default { throw "Invalid action: $Action" }
}
