# Báo Cáo Lab Spark - Phân Tích Dữ Liệu Phim

## Thông Tin
- **Môn học**: IE212 - Big Data
- **Họ tên sinh viên**: Nguyễn Anh Tuấn
- **MSSV**: 23521716

---

## Môi Trường Cài Đặt

### Yêu Cầu
- **Java**: JDK 17 trở lên
- **Python**: 3.11
- **Spark**: 3.4.1
- **OS**: Windows 10/11 (với PowerShell 5.1)

### Cài Đặt Spark & PySpark

1. **Tải Spark 3.4.1**:
   ```bash
   # Tải từ https://archive.apache.org/dist/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz
   # Giải nén vào thư mục dự án
   ```

2. **Tạo Python Virtual Environment**:
   ```bash
   python -m venv venv
   .\venv\Scripts\Activate.ps1
   ```

3. **Cài đặt PySpark**:
   ```bash
   pip install pyspark==3.4.1
   ```

---

## Cấu Trúc Thư Mục

```
Lab2/
├── Bai1.py              # Phân tích đánh giá trung bình phim
├── Bai2.py              # Phân tích đánh giá theo thể loại
├── Bai3.py              # Phân tích đánh giá theo giới tính
├── Bai4.py              # Phân tích đánh giá theo nhóm tuổi
├── Bai5.py              # Phân tích đánh giá theo nghề nghiệp
├── Bai6.py              # Phân tích đánh giá theo thời gian
├── run_bai1.ps1         # Script chạy Bai1
├── run_bai2.ps1         # Script chạy Bai2
├── run_bai3.ps1         # Script chạy Bai3
├── run_bai4.ps1         # Script chạy Bai4
├── run_bai5.ps1         # Script chạy Bai5
├── run_bai6.ps1         # Script chạy Bai6
├── movies.txt           # Dữ liệu phim
├── ratings_1.txt        # Dữ liệu đánh giá (phần 1)
├── ratings_2.txt        # Dữ liệu đánh giá (phần 2)
├── users.txt            # Dữ liệu người dùng
├── occupation.txt       # Danh sách nghề nghiệp
└── README.md            # Tài liệu này
```

---

## Format Dữ Liệu

### movies.txt
- **Schema**: `MovieID,Title,Genres`
- **Ví dụ**: `1001,The Godfather (1972),Crime|Drama`

### ratings_1.txt, ratings_2.txt
- **Schema**: `UserID,MovieID,Rating,Timestamp`
- **Ví dụ**: `1,1001,4.5,1577836800`

### users.txt
- **Schema**: `UserID,Gender,Age,Occupation,ZipCode`
- **Ví dụ**: `1,M,28,3,12345`

### occupation.txt
- **Schema**: `ID,Occupation`
- **Ví dụ**: `3,Sales/Marketing`

---

## Cách Chạy Các Bài Tập

### Chạy từng bài (Windows PowerShell)

```bash
# Bài 1: Phân tích đánh giá trung bình phim
.\run_bai1.ps1

# Bài 2: Phân tích đánh giá theo thể loại
.\run_bai2.ps1

# Bài 3: Phân tích đánh giá theo giới tính
.\run_bai3.ps1

# Bài 4: Phân tích đánh giá theo nhóm tuổi
.\run_bai4.ps1

# Bài 5: Phân tích đánh giá theo nghề nghiệp
.\run_bai5.ps1

# Bài 6: Phân tích đánh giá theo thời gian
.\run_bai6.ps1
```

---

## Kết Quả Từng Bài

### Bài 1: Phân Tích Đánh Giá Trung Bình Của Phim

**Mục tiêu**: Tính điểm đánh giá trung bình và xác định phim được đánh giá cao nhất

**Output Format**: 
```
MovieTitle AverageRating: x.xx (TotalRatings: n)
[Phim được đánh giá cao nhất]
```

**Kết quả**:
```
Sunset Boulevard (1950) AverageRating: 4.36 (TotalRatings: 7)
Psycho (1960) AverageRating: 4.00 (TotalRatings: 2)
The Silence of the Lambs (1991) AverageRating: 3.14 (TotalRatings: 7)
No Country for Old Men (2007) AverageRating: 3.89 (TotalRatings: 18)
The Lord of the Rings: The Return of the King (2003) AverageRating: 3.82 (TotalRatings: 11)
E.T. the Extra-Terrestrial (1982) AverageRating: 3.67 (TotalRatings: 18)
Mad Max: Fury Road (2015) AverageRating: 3.47 (TotalRatings: 18)
The Terminator (1984) AverageRating: 4.06 (TotalRatings: 18)
The Godfather: Part II (1974) AverageRating: 4.00 (TotalRatings: 17)
The Lord of the Rings: The Fellowship of the Ring (2001) AverageRating: 3.89 (TotalRatings: 18)
Gladiator (2000) AverageRating: 3.61 (TotalRatings: 18)
The Social Network (2010) AverageRating: 3.86 (TotalRatings: 7)
Lawrence of Arabia (1962) AverageRating: 3.44 (TotalRatings: 18)
Fight Club (1999) AverageRating: 3.50 (TotalRatings: 7)

Sunset Boulevard (1950) is the highest rated movie with an average rating of 4.36 among movies with at least 5 ratings.
```

---

### Bài 2: Phân Tích Đánh Giá Theo Thể Loại

**Mục tiêu**: Tính điểm đánh giá trung bình cho từng thể loại phim

**Output Format**: 
```
Genre, Average Rating: x.xx
```

**Kết quả**:
```
Genre: Sci-Fi, Average Rating: 3.73
Genre: Action, Average Rating: 3.71
Genre: Drama, Average Rating: 3.75
Genre: Family, Average Rating: 3.67
Genre: Biography, Average Rating: 3.65
Genre: Horror, Average Rating: 4.00
Genre: Fantasy, Average Rating: 3.85
Genre: Mystery, Average Rating: 4.00
Genre: Thriller, Average Rating: 3.68
Genre: Adventure, Average Rating: 3.65
Genre: Film-Noir, Average Rating: 4.36
Genre: Crime, Average Rating: 3.68
```

---

### Bài 3: Phân Tích Đánh Giá Theo Giới Tính

**Mục tiêu**: Tính điểm đánh giá trung bình cho từng phim theo giới tính (Nam/Nữ)

**Output Format**: 
```
MovieTitle - Male_Avg: x.xx, Female_Avg: x.xx
```

**Kết quả**:
```
E.T. the Extra-Terrestrial (1982) - Male_Avg: 3.81, Female_Avg: 3.55
Fight Club (1999) - Male_Avg: 3.50, Female_Avg: 3.50
Gladiator (2000) - Male_Avg: 3.59, Female_Avg: 3.64
Lawrence of Arabia (1962) - Male_Avg: 3.55, Female_Avg: 3.31
Mad Max: Fury Road (2015) - Male_Avg: 4.00, Female_Avg: 3.32
No Country for Old Men (2007) - Male_Avg: 3.92, Female_Avg: 3.83
Psycho (1960) - Male_Avg: NA, Female_Avg: 4.00
Sunset Boulevard (1950) - Male_Avg: 4.33, Female_Avg: 4.50
The Godfather: Part II (1974) - Male_Avg: 4.06, Female_Avg: 3.94
The Lord of the Rings: The Fellowship of the Ring (2001) - Male_Avg: 4.00, Female_Avg: 3.80
The Lord of the Rings: The Return of the King (2003) - Male_Avg: 3.75, Female_Avg: 3.90
The Silence of the Lambs (1991) - Male_Avg: 3.33, Female_Avg: 3.00
The Social Network (2010) - Male_Avg: 4.00, Female_Avg: 3.67
The Terminator (1984) - Male_Avg: 3.93, Female_Avg: 4.14
```

---

### Bài 4: Phân Tích Đánh Giá Theo Nhóm Tuổi

**Mục tiêu**: Tính điểm đánh giá trung bình cho từng phim theo nhóm tuổi (0-18, 18-35, 35-50, 50+)

**Output Format**: 
```
MovieTitle - [0-18: AvgRating, 18-35: AvgRating, 35-50: AvgRating, 50+: AvgRating]
```

**Kết quả**:
```
E.T. the Extra-Terrestrial (1982) - [0-18: NA, 18-35: 3.56, 35-50: 3.83, 50+: 3.00]
Fight Club (1999) - [0-18: NA, 18-35: 3.50, 35-50: 3.50, 50+: 3.50]
Gladiator (2000) - [0-18: NA, 18-35: 3.43, 35-50: 3.81, 50+: 3.50]
Lawrence of Arabia (1962) - [0-18: NA, 18-35: 3.60, 35-50: 3.32, 50+: 3.75]
Mad Max: Fury Road (2015) - [0-18: NA, 18-35: 3.36, 35-50: 3.64, 50+: NA]
No Country for Old Men (2007) - [0-18: NA, 18-35: 3.79, 35-50: 3.88, 50+: 4.17]
Psycho (1960) - [0-18: NA, 18-35: 4.50, 35-50: 3.50, 50+: NA]
Sunset Boulevard (1950) - [0-18: NA, 18-35: 4.17, 35-50: 4.50, 50+: 4.50]
The Godfather: Part II (1974) - [0-18: NA, 18-35: 3.78, 35-50: 4.25, 50+: NA]
The Lord of the Rings: The Fellowship of the Ring (2001) - [0-18: NA, 18-35: 4.00, 35-50: 3.75, 50+: 4.25]
The Lord of the Rings: The Return of the King (2003) - [0-18: NA, 18-35: 3.83, 35-50: 3.86, 50+: 3.50]
The Silence of the Lambs (1991) - [0-18: NA, 18-35: 3.00, 35-50: 3.00, 50+: 4.00]
The Social Network (2010) - [0-18: NA, 18-35: 4.00, 35-50: 3.67, 50+: NA]
The Terminator (1984) - [0-18: NA, 18-35: 4.17, 35-50: 4.06, 50+: 3.83]
```

---

### Bài 5: Phân Tích Đánh Giá Theo Nghề Nghiệp

**Mục tiêu**: Tính điểm đánh giá trung bình cho từng nghề nghiệp

**Output Format**: 
```
Occupation - AverageRating: x.xx (TotalRatings: n)
```

**Kết quả**:
```
Lawyer - AverageRating: 3.65 (TotalRatings: 17)
Accountant - AverageRating: 3.58 (TotalRatings: 6)
Teacher - AverageRating: 3.70 (TotalRatings: 5)
Designer - AverageRating: 4.00 (TotalRatings: 13)
Doctor - AverageRating: 3.69 (TotalRatings: 21)
Salesperson - AverageRating: 3.65 (TotalRatings: 17)
Programmer - AverageRating: 4.25 (TotalRatings: 10)
Consultant - AverageRating: 3.86 (TotalRatings: 14)
Manager - AverageRating: 3.47 (TotalRatings: 16)
Artist - AverageRating: 3.73 (TotalRatings: 11)
Student - AverageRating: 4.00 (TotalRatings: 8)
Engineer - AverageRating: 3.56 (TotalRatings: 18)
Nurse - AverageRating: 3.86 (TotalRatings: 11)
Journalist - AverageRating: 3.85 (TotalRatings: 17)
```

---

### Bài 6: Phân Tích Đánh Giá Theo Thời Gian

**Mục tiêu**: Tính tổng số lượt đánh giá và điểm đánh giá trung bình cho từng năm

**Output Format**: 
```
Year - TotalRatings: xx, AverageRating: xx
```

**Kết quả**:
```
2020 - TotalRatings: 184, AverageRating: 3.75
```

---

## Kiến Thức Rút Ra

1. **Đọc kỹ schema của dữ liệu** trước khi viết code
2. **Index đúng** khi parse dữ liệu từ file text
3. **Sử dụng RDD operations** (map, filter, reduceByKey, join) để xử lý dữ liệu phân tán
4. **Collect dữ liệu về local** trước khi xử lý cuối cùng (tránh worker crash trên Windows)
5. **Format output** đúng theo yêu cầu bài tập

---

## Ghi Chú

- Trên Windows, cần thiết lập `PYSPARK_PYTHON` và `PYSPARK_DRIVER_PYTHON` environment variables
- Spark master chạy ở chế độ `local[*]` để sử dụng toàn bộ CPU cores
- Các file ratings được gộp từ 2 file (ratings_1.txt và ratings_2.txt) bằng RDD union
