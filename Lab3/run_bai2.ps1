# Script khởi chạy Bai2.py với cấu hình đúng cho Spark/PySpark trên Windows

# Kích hoạt venv (venv ở cấp Lab)
& "$PSScriptRoot\..\venv\Scripts\Activate.ps1"

# Đặt biến môi trường cho Spark
$env:PYSPARK_PYTHON = "$PSScriptRoot\..\venv\Scripts\python.exe"
$env:PYSPARK_DRIVER_PYTHON = "$PSScriptRoot\..\venv\Scripts\python.exe"

# Chạy code
python "$PSScriptRoot\Bai2.py"
