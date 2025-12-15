# Script to run Bai4.py with proper Spark/PySpark configuration on Windows
# Activates venv and sets environment variables

# Activate venv
& "$PSScriptRoot\venv\Scripts\Activate.ps1"

# Set environment variables for Spark
$env:PYSPARK_PYTHON = "$PSScriptRoot\venv\Scripts\python.exe"
$env:PYSPARK_DRIVER_PYTHON = "$PSScriptRoot\venv\Scripts\python.exe"

# Run code
python "$PSScriptRoot\Bai4.py"
