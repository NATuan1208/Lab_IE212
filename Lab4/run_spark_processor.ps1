# Script khoi chay Spark Processor

# Kich hoat venv (venv o cap Lab)
& "$PSScriptRoot\..\venv\Scripts\Activate.ps1"

# Dat bien moi truong cho PySpark
$env:PYSPARK_PYTHON = "$PSScriptRoot\..\venv\Scripts\python.exe"
$env:PYSPARK_DRIVER_PYTHON = "$PSScriptRoot\..\venv\Scripts\python.exe"

# Thiet lap Hadoop Home de tranh loi tren Windows
$hadoopHome = "$PSScriptRoot\hadoop"
$winutilsPath = "$hadoopHome\bin\winutils.exe"
$hadoopDllPath = "$hadoopHome\bin\hadoop.dll"

# Kiem tra va tai winutils.exe va hadoop.dll neu chua co
if (-not (Test-Path $winutilsPath) -or -not (Test-Path $hadoopDllPath)) {
    Write-Host "[Setup] Dang tao thu muc Hadoop..."
    New-Item -ItemType Directory -Path "$hadoopHome\bin" -Force | Out-Null
    
    $baseUrl = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin"
    
    if (-not (Test-Path $winutilsPath)) {
        Write-Host "[Setup] Dang tai winutils.exe..."
        try {
            Invoke-WebRequest -Uri "$baseUrl/winutils.exe" -OutFile $winutilsPath -UseBasicParsing
            Write-Host "[Setup] Da tai winutils.exe thanh cong!"
        } catch {
            Write-Host "[WARNING] Khong the tai winutils.exe"
        }
    }
    
    if (-not (Test-Path $hadoopDllPath)) {
        Write-Host "[Setup] Dang tai hadoop.dll..."
        try {
            Invoke-WebRequest -Uri "$baseUrl/hadoop.dll" -OutFile $hadoopDllPath -UseBasicParsing
            Write-Host "[Setup] Da tai hadoop.dll thanh cong!"
        } catch {
            Write-Host "[WARNING] Khong the tai hadoop.dll"
        }
    }
}

$env:HADOOP_HOME = $hadoopHome
$env:PATH = "$hadoopHome\bin;$env:PATH"
Write-Host "[Setup] HADOOP_HOME = $env:HADOOP_HOME"

# Chay Spark Processor
python "$PSScriptRoot\spark_processor.py"
