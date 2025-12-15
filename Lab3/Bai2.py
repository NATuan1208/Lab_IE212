"""
Bài 2: Thống kê tổng số đơn hàng, khách hàng và người bán
---------------------------------------------------------
Mục tiêu:
- Đếm tổng số đơn hàng (Orders)
- Đếm tổng số khách hàng (Customers)  
- Đếm tổng số người bán (Sellers)

Kiến thức sử dụng:
- count(): Đếm số dòng trong DataFrame
- distinct(): Loại bỏ các dòng trùng lặp
- countDistinct(): Đếm số giá trị duy nhất trong một cột
- agg(): Aggregate function - thực hiện tính toán tổng hợp
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct

# Khởi tạo Spark Session
spark = SparkSession.builder.appName("Lab3_Bai2").getOrCreate()

# Định nghĩa đường dẫn đến các file dữ liệu
# Sử dụng dictionary để dễ quản lý và gọi
data_paths = {
    "customer_list": './data/Customer_List.csv',
    "order_item": './data/Order_Items.csv',
    "order_review": './data/Order_Reviews.csv',
    "order": './data/Orders.csv',
    "product": './data/Products.csv'
}

print("Bat dau thong ke...\n")

# Thong ke tong so don hang
print("1. Dang dem so don hang...")

# Đọc file Orders.csv
# Mỗi dòng = 1 đơn hàng
df_orders = spark.read.csv(data_paths['order'], header=True, sep=";")

# Cách 1: Đếm tất cả các dòng (bao gồm cả trùng lặp nếu có)
total_orders = df_orders.count()

# Cách 2: Loại bỏ trùng lặp trước khi đếm (an toàn hơn)
# distinct() loại bỏ các dòng hoàn toàn giống nhau
total_orders_distinct = df_orders.distinct().count()

print(f"   Tong so don hang: {total_orders_distinct}")

# Thong ke tong so khach hang
print("2. Dang dem so khach hang...")

# Đọc file Customer_List.csv
# Mỗi dòng = 1 khách hàng
df_customers = spark.read.csv(data_paths['customer_list'], header=True, sep=";")

# Sử dụng distinct() để loại bỏ khách hàng trùng lặp (nếu có)
total_customers = df_customers.distinct().count()

print(f"   Tong so khach hang: {total_customers}")

# Thong ke tong so nguoi ban
print("3. Dang dem so nguoi ban...")

# Đọc file Order_Items.csv
# File này chứa thông tin về sản phẩm trong mỗi đơn hàng
# Một người bán (Seller_ID) có thể bán nhiều sản phẩm
df_order_items = spark.read.csv(data_paths['order_item'], header=True, sep=";")

# QUAN TRỌNG: Phải đếm số Seller_ID DUY NHẤT (DISTINCT)
# Cách 1: Sử dụng countDistinct() - Gọn gàng nhất
total_sellers = df_order_items.agg(countDistinct("Seller_ID")).collect()[0][0]

# Giải thích cách 1:
# - agg(countDistinct("Seller_ID")): Đếm số giá trị duy nhất trong cột Seller_ID
# - collect(): Lấy kết quả về driver (máy chủ)
# - [0][0]: Lấy giá trị đầu tiên của dòng đầu tiên

# Cách 2: Sử dụng select().distinct().count() - Dễ hiểu hơn
# total_sellers_v2 = df_order_items.select("Seller_ID").distinct().count()

print(f"   Tong so nguoi ban: {total_sellers}")

# Ghi ket qua ra file
print("\nDang luu ket qua...")

output_path = './result/result_bai2.txt'

with open(output_path, 'w', encoding='utf-8') as f:
    f.write("=" * 50 + "\n")
    f.write("THONG KE TONG QUAN HE THONG E-COMMERCE\n")
    f.write("=" * 50 + "\n\n")
    f.write(f"Tong so don hang: {total_orders_distinct:,}\n")
    f.write(f"Tong so khach hang: {total_customers:,}\n")
    f.write(f"Tong so nguoi ban: {total_sellers:,}\n")
    f.write("\n" + "=" * 50 + "\n")

# In ket qua ra console
print("\n" + "=" * 50)
print("KET QUA THONG KE:")
print("=" * 50)
print(f"Tong so don hang: {total_orders_distinct:,}")
print(f"Tong so khach hang: {total_customers:,}")
print(f"Tong so nguoi ban: {total_sellers:,}")
print("=" * 50)

# Dong Spark Session
spark.stop()

print(f"\nHoan thanh! Ket qua da duoc luu vao: {output_path}")
