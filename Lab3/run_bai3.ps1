# Script khoi chay Bai3.py voi cau hinh dung cho Spark/PySpark tren Windows

# Kich hoat venv (venv o cap Lab)
& "$PSScriptRoot\..\venv\Scripts\Activate.ps1"

# Dat bien moi truong cho Spark
$env:PYSPARK_PYTHON = "$PSScriptRoot\..\venv\Scripts\python.exe"
$env:PYSPARK_DRIVER_PYTHON = "$PSScriptRoot\..\venv\Scripts\python.exe"

# Chay code
python "$PSScriptRoot\Bai3.py"
