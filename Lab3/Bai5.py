"""
Bai 5: Thong ke diem danh gia cua khach hang
---------------------------------------------
Muc tieu:
- Tinh diem danh gia trung binh
- Dem so luong danh gia theo tung muc (1-5 sao)
- Xu ly cac gia tri ngoai le va NULL trong cot Review_Score

Kien thuc su dung:
- avg(): Tinh trung binh
- cast(): Chuyen doi kieu du lieu
- filter(): Loc du lieu
- isNotNull(): Kiem tra khong NULL
- expr(): Su dung bieu thuc SQL trong DataFrame
- try_cast(): Cast an toan, tra ve NULL neu loi
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, expr

# Khoi tao Spark Session
spark = SparkSession.builder.appName("Lab3_Bai5").getOrCreate()

# Dinh nghia duong dan den file danh gia
data_paths = {
    "customer_list": './data/Customer_List.csv',
    "order_item": './data/Order_Items.csv',
    "order_review": './data/Order_Reviews.csv',
    "order": './data/Orders.csv',
    "product": './data/Products.csv'
}

print("Bat dau phan tich danh gia cua khach hang...\n")

# Doc file danh gia
print("1. Dang doc du lieu danh gia...")
df_review = spark.read.csv(data_paths['order_review'], header=True, sep=";")

# Xu ly du lieu: Chuyen Review_Score sang kieu integer
print("2. Dang xu ly va loc du lieu...")

# try_cast: Thu chuyen doi sang int, neu that bai thi tra ve NULL
# expr() cho phep viet bieu thuc SQL trong DataFrame API
df_review = df_review.withColumn("Score", expr("try_cast(Review_Score as int)"))

# Loc chi lay cac gia tri hop le
# - Score khong NULL
# - Score >= 1 va <= 5 (chi chap nhan diem tu 1 den 5 sao)
df_clean = df_review.filter(
    col("Score").isNotNull() & 
    (col("Score") >= 1) & 
    (col("Score") <= 5)
)

# Dem so ban ghi truoc va sau khi loc
total_before = df_review.count()
total_after = df_clean.count()
filtered_out = total_before - total_after

print(f"   Tong so danh gia ban dau: {total_before:,}")
print(f"   Sau khi loc: {total_after:,}")
print(f"   Da loai bo: {filtered_out:,} danh gia khong hop le\n")

# Tinh diem trung binh
print("3. Dang tinh diem trung binh...")
avg_score = df_clean.agg(avg("Score")).collect()[0][0]
print(f"   Diem trung binh: {avg_score:.4f}\n")

# Dem so luong danh gia theo tung muc
print("4. Dang thong ke so luong danh gia theo muc...")

# groupBy("Score"): Nhom theo diem
# count("*"): Dem tat ca cac dong trong moi nhom
# orderBy("Score"): Sap xep theo diem tang dan
df_count = df_clean.groupBy("Score") \
                   .agg(count("*").alias("Total_Reviews")) \
                   .orderBy("Score")

# Lay ket qua
results = df_count.collect()

# Ghi ket qua ra file
output_path = './result/result_bai5.txt'

with open(output_path, 'w', encoding='utf-8') as f:
    f.write("=" * 60 + "\n")
    f.write("THONG KE DANH GIA CUA KHACH HANG\n")
    f.write("=" * 60 + "\n\n")
    f.write(f"Diem danh gia trung binh: {avg_score:.4f}\n\n")
    f.write("Phan bo danh gia theo muc:\n")
    f.write("-" * 60 + "\n")
    
    for row in results:
        score = row['Score']
        total = row['Total_Reviews']
        percentage = (total / total_after) * 100
        
        # Ve thanh bieu do don gian
        bar = '*' * int(percentage / 2)
        f.write(f"{score} sao: {total:6,} danh gia ({percentage:5.2f}%) {bar}\n")
    
    f.write("\n" + "=" * 60 + "\n")
    f.write(f"Tong so danh gia hop le: {total_after:,}\n")
    f.write(f"So danh gia bi loai: {filtered_out:,}\n")
    f.write("=" * 60 + "\n")

# In ket qua ra console
print("\n" + "=" * 60)
print("KET QUA THONG KE DANH GIA:")
print("=" * 60)
print(f"Diem trung binh: {avg_score:.4f}\n")
print("Phan bo theo muc:")
print("-" * 60)

for row in results:
    score = row['Score']
    total = row['Total_Reviews']
    percentage = (total / total_after) * 100
    bar = '*' * int(percentage / 2)
    print(f"{score} sao: {total:6,} danh gia ({percentage:5.2f}%) {bar}")

print("=" * 60)

# Dong Spark Session
spark.stop()

print(f"\nHoan thanh! Ket qua da duoc luu vao: {output_path}")
