"""
Bai 4: Phan tich so luong don hang theo nam, thang
----------------------------------------------------
Muc tieu:
- Nhom don hang theo nam va thang dat hang
- Sap xep theo nam tang dan, thang giam dan

Kien thuc su dung:
- year(): Trích xuat nam tu timestamp
- month(): Trích xuat thang tu timestamp  
- groupBy(): Nhom du lieu theo nhieu cot
- orderBy() / sort(): Sap xep theo nhieu cot
- asc(): Sap xep tang dan
- desc(): Sap xep giam dan
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, year, month, asc, desc

# Khoi tao Spark Session
spark = SparkSession.builder.appName("Lab3_Bai4").getOrCreate()

# Dinh nghia duong dan den file don hang
data_paths = {
    "customer_list": './data/Customer_List.csv',
    "order_item": './data/Order_Items.csv',
    "order_review": './data/Order_Reviews.csv',
    "order": './data/Orders.csv',
    "product": './data/Products.csv'
}

print("Bat dau phan tich don hang theo thoi gian...\n")

# Doc file don hang
print("1. Dang doc du lieu don hang...")
df_orders = spark.read.csv(data_paths['order'], header=True, sep=";")

# Xu ly va nhom du lieu
print("2. Dang trich xuat nam, thang tu thoi gian dat hang...")

# withColumn(): Them cot moi vao DataFrame
# year("Order_Purchase_Timestamp"): Trích xuat nam tu cot timestamp
# month("Order_Purchase_Timestamp"): Trích xuat thang tu cot timestamp
df_with_time = df_orders.withColumn("Year", year("Order_Purchase_Timestamp")) \
                        .withColumn("Month", month("Order_Purchase_Timestamp"))

# Nhom theo nam va thang, dem so don hang
print("3. Dang nhom va dem so don hang...")

# groupBy("Year", "Month"): Nhom theo ca nam va thang
# count("Order_ID"): Dem so don hang trong moi nhom
# orderBy(asc("Year"), desc("Month")): Sap xep nam tang dan, thang giam dan
df_result = df_with_time.groupBy("Year", "Month") \
                        .agg(count("Order_ID").alias("Total_Orders")) \
                        .orderBy(asc("Year"), desc("Month"))

# Lay ket qua
print("4. Dang lay ket qua...\n")
results = df_result.collect()

# Ghi ket qua ra file
output_path = './result/result_bai4.txt'

with open(output_path, 'w', encoding='utf-8') as f:
    f.write("=" * 60 + "\n")
    f.write("PHAN TICH SO LUONG DON HANG THEO NAM, THANG\n")
    f.write("=" * 60 + "\n\n")
    
    current_year = None
    for row in results:
        year_val = row['Year']
        month_val = row['Month']
        total = row['Total_Orders']
        
        # In header cho moi nam moi
        if current_year != year_val:
            if current_year is not None:
                f.write("\n")
            f.write(f"NAM {year_val}:\n")
            f.write("-" * 60 + "\n")
            current_year = year_val
        
        f.write(f"  Thang {month_val:2}: {total:,} don hang\n")
    
    f.write("\n" + "=" * 60 + "\n")

# In ket qua ra console (20 dong dau tien)
print("=" * 60)
print("PHAN TICH DON HANG THEO THOI GIAN (20 DONG DAU):")
print("=" * 60)

current_year = None
count_lines = 0
for row in results:
    if count_lines >= 20:
        break
    
    year_val = row['Year']
    month_val = row['Month']
    total = row['Total_Orders']
    
    if current_year != year_val:
        if current_year is not None:
            print()
        print(f"NAM {year_val}:")
        print("-" * 60)
        current_year = year_val
        count_lines += 2
    
    print(f"  Thang {month_val:2}: {total:,} don hang")
    count_lines += 1

print("=" * 60)

# Dong Spark Session
spark.stop()

print(f"\nHoan thanh! Ket qua da duoc luu vao: {output_path}")
