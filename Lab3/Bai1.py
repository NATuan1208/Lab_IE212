"""
Bài 1: Đọc và Hiển Thị Schema của Các File CSV
----------------------------------------------
Mục tiêu:
- Đọc các file CSV với tùy chọn inferSchema=True để Spark tự động suy ra kiểu dữ liệu
- Hiển thị schema (cấu trúc dữ liệu) của từng file
"""

from pyspark.sql import SparkSession

# Khởi tạo Spark Session
# SparkSession là điểm vào chính để làm việc với Spark DataFrame
spark = SparkSession.builder.appName("Lab3_Bai1").getOrCreate()

# Định nghĩa đường dẫn đến các file dữ liệu
data_paths = [
    './data/Customer_List.csv',
    './data/Order_Items.csv', 
    './data/Order_Reviews.csv',
    './data/Orders.csv',
    './data/Products.csv'
]

# Chuỗi lưu trữ kết quả schema
schema_output = ''

# Duyệt qua từng file CSV
for path in data_paths:
    # Đọc file CSV với các tham số:
    # - header=True: Dòng đầu tiên là tên cột
    # - inferSchema=True: Tự động suy ra kiểu dữ liệu (int, string, date, timestamp, double...)
    # - sep=";": Dấu phân cách là dấu chấm phẩy (;)
    df = spark.read.csv(path, header=True, inferSchema=True, sep=";")
    
    # Lấy tên file (loại bỏ phần đường dẫn)
    file_name = path.replace('./', '')
    schema_output += f"{file_name}\n"
    
    # Duyệt qua từng field trong schema
    for field in df.schema:
        # field.name: Tên cột
        # field.dataType.simpleString(): Kiểu dữ liệu dưới dạng chuỗi
        schema_output += f"  - {field.name}: {field.dataType.simpleString()}\n"
    
    schema_output += "\n\n"

# Ghi kết quả ra file
with open('./result/result_bai1.txt', 'w', encoding='utf-8') as f:
    f.write(schema_output)

# In kết quả ra console
print(schema_output)

# Đóng Spark Session
spark.stop()

print("Hoàn thành! Kết quả đã được lưu vào file result/result_bai1.txt")
