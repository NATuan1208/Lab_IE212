# Script khởi chạy Bai1.py với cấu hình đúng cho Spark/PySpark trên Windows
# Giải quyết lỗi: đường dẫn chứa khoảng trắng, Python worker crash

# Kích hoạt venv
& "$PSScriptRoot\venv\Scripts\Activate.ps1"

# Đặt biến môi trường cho Spark
$env:PYSPARK_PYTHON = "$PSScriptRoot\venv\Scripts\python.exe"
$env:PYSPARK_DRIVER_PYTHON = "$PSScriptRoot\venv\Scripts\python.exe"

# Chạy code
python "$PSScriptRoot\Bai2.py"
# 