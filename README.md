# Lab 2: Big Data Processing with PySpark RDD

## Mục tiêu
Thực hiện các bài tập xử lý dữ liệu phim bằng **PySpark RDD** (Resilient Distributed Datasets).

## Yêu cầu hệ thống
- **Python**: 3.11 hoặc cao hơn
- **Java**: JDK 11 hoặc JDK 17
- **Spark**: 3.4.1 trở lên
- **PySpark**: 3.4.1 hoặc 3.5.1+

## Cấu trúc file

```
Lab2/
├── Bai1.py                  # Bài 1: Tính điểm đánh giá phim
├── run_bai1.ps1             # Script chạy Bài 1 (PowerShell)
├── ratings_1.txt            # File dữ liệu đánh giá phim 1
├── ratings_2.txt            # File dữ liệu đánh giá phim 2
├── movies.txt               # File danh sách phim
├── users.txt                # File danh sách người dùng
├── occupation.txt           # File danh sách công việc
└── README.md                # File này
```

## Hướng dẫn cài đặt

### 1. Cài đặt môi trường Python

**Tạo virtual environment:**
```powershell
cd "D:\SinhVien\UIT_HocChinhKhoa\HK1 2025 - 2026\Bigdata_IE212\Lab2"
py -3.11 -m venv venv
.\venv\Scripts\Activate.ps1
```

**Cài đặt PySpark:**
```powershell
pip install --upgrade pip
pip install pyspark==3.4.1
```

### 2. Kiểm tra Java

Đảm bảo Java đã được cài đặt:
```powershell
java -version
```

Nếu chưa, cài đặt **JDK 17** từ [oracle.com](https://www.oracle.com/java/technologies/downloads/)

### 3. Đặt biến môi trường JAVA_HOME (nếu cần)

Nếu Java không được nhận dạng:
```powershell
$env:JAVA_HOME = "C:\Program Files\Java\jdk-17"
```

## Hướng dẫn chạy

### Cách 1: Dùng PowerShell script (Cách dễ nhất)

```powershell
cd "D:\SinhVien\UIT_HocChinhKhoa\HK1 2025 - 2026\Bigdata_IE212\Lab2"
.\run_bai1.ps1
```

### Cách 2: Chạy thủ công (PowerShell)

```powershell
cd "D:\SinhVien\UIT_HocChinhKhoa\HK1 2025 - 2026\Bigdata_IE212\Lab2"
.\venv\Scripts\Activate.ps1
$pythonPath = "$(Get-Location)\venv\Scripts\python.exe"
$env:PYSPARK_PYTHON = $pythonPath
$env:PYSPARK_DRIVER_PYTHON = $pythonPath
python Bai1.py
```

## Nội dung các bài tập

### **Bài 1: Tính điểm đánh giá phim**

**Yêu cầu:**
- Đọc dữ liệu từ `ratings_1.txt` và `ratings_2.txt`
- Tính **trung bình điểm đánh giá** cho mỗi bộ phim
- Ghép dữ liệu với tên phim từ `movies.txt`
- In danh sách phim theo thứ tự alphabetical
- Tìm phim có **điểm trung bình cao nhất** (ít nhất 5 lượt đánh giá)

**Công nghệ sử dụng:**
- **RDD operations**: `map()`, `filter()`, `union()`, `reduceByKey()`, `mapValues()`, `leftOuterJoin()`
- **Spark Context**: `textFile()`, `collect()`

**Output mẫu:**
```
=== Bài 1: Tính điểm đánh giá phim (dùng RDD) ===
Phiên bản Spark: 3.4.1

=== Danh sách phim và điểm đánh giá ===
E.T. the Extra-Terrestrial (1982) Điểm trung bình: 3.67 (Tổng lượt đánh giá: 18)
Fight Club (1999) Điểm trung bình: 3.5 (Tổng lượt đánh giá: 7)
...
Sunset Boulevard (1950) Điểm trung bình: 4.36 (Tổng lượt đánh giá: 7)

=== Kết quả ===
Sunset Boulevard (1950) là phim có điểm trung bình cao nhất: 4.36 (tối thiểu 5 lượt đánh giá)
```

**Format file dữ liệu:**

- `ratings_1.txt`, `ratings_2.txt`: `userId,movieId,rating,timestamp`
- `movies.txt`: `movieId,title,genres`
- `users.txt`: `userId,age,gender,occupation,zipCode`

## Cấu trúc code Bài 1

```python
def parse_rating(line):
    """Parse dòng đánh giá thành (movieId, (rating, 1))"""
    
def parse_movie(line):
    """Parse dòng phim thành (movieId, title)"""
    
def main():
    # 1. Khởi tạo SparkSession
    # 2. Đọc files → RDD
    # 3. Parse ratings → RDD
    # 4. Tính tổng/đếm bằng reduceByKey()
    # 5. Tính trung bình bằng mapValues()
    # 6. Ghép với movies bằng leftOuterJoin()
    # 7. Collect() về local
    # 8. Sắp xếp và in kết quả trên Python
```

## Ghi chú quan trọng

### ⚠️ Lỗi thường gặp

1. **"Python was not found"**
   - Nguyên nhân: Biến môi trường `PYSPARK_PYTHON` chưa đặt
   - Giải pháp: Đặt `$env:PYSPARK_PYTHON` trước khi chạy

2. **"Python worker failed to connect back"**
   - Nguyên nhân: Spark không tìm được Python executable
   - Giải pháp: Sử dụng đường dẫn tuyệt đối cho PYSPARK_PYTHON

3. **"HADOOP_HOME and hadoop.home.dir are unset"**
   - Nguyên nhân: Spark cảnh báo về Hadoop (có thể bỏ qua trên Windows)
   - Giải pháp: Cảnh báo này không ảnh hưởng đến kết quả, có thể bỏ qua

### ℹ️ Tại sao dùng RDD?

- **RDD** là abstraction cơ bản của Spark, phù hợp cho xử lý dữ liệu phức tạp
- Hỗ trợ **transformations** (map, filter, reduceByKey) và **actions** (collect, count)
- Phù hợp với yêu cầu lab: "Sử dụng RDD và PySpark"

### 💡 Tips tối ưu hóa

1. Dùng `reduceByKey()` thay vì `groupByKey()` vì nó tối ưu hơn (pre-aggregation)
2. Gọi `collect()` ở cuối để tránh overhead của distributed operations trên dữ liệu nhỏ
3. Luôn gọi `spark.stop()` để giải phóng tài nguyên

## Troubleshooting

### Script PowerShell không chạy được?
Nếu gặp lỗi permission:
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### Venv không activate?
Kiểm tra đường dẫn:
```powershell
Test-Path ".\venv\Scripts\Activate.ps1"
```

---

**Cập nhật**: 23/11/2025 | **Spark Version**: 3.4.1 | **Python**: 3.11+
