# Script khoi chay Camera Server

# Kich hoat venv (venv o cap Lab)
& "$PSScriptRoot\..\venv\Scripts\Activate.ps1"

# Dat bien moi truong
$env:PYSPARK_PYTHON = "$PSScriptRoot\..\venv\Scripts\python.exe"
$env:PYSPARK_DRIVER_PYTHON = "$PSScriptRoot\..\venv\Scripts\python.exe"

# Chay Camera Server
python "$PSScriptRoot\camera_server.py"
