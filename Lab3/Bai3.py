"""
Bai 3: Phan tich so luong don hang theo quoc gia
-------------------------------------------------
Muc tieu:
- Thong ke so luong don hang theo tung quoc gia
- Sap xep theo thu tu giam dan (quoc gia co nhieu don hang nhat o tren)

Kien thuc su dung:
- join(): Ket noi hai DataFrame
- groupBy(): Nhom du lieu theo cot
- count(): Dem so luong
- orderBy() / sort(): Sap xep ket qua
- desc(): Sap xep giam dan
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc

# Khoi tao Spark Session
spark = SparkSession.builder.appName("Lab3_Bai3").getOrCreate()

# Dinh nghia duong dan den cac file du lieu
data_paths = {
    "customer_list": './data/Customer_List.csv',
    "order_item": './data/Order_Items.csv',
    "order_review": './data/Order_Reviews.csv',
    "order": './data/Orders.csv',
    "product": './data/Products.csv'
}

print("Bat dau phan tich don hang theo quoc gia...\n")

# Doc file khach hang (co thong tin quoc gia)
print("1. Dang doc du lieu khach hang...")
df_customers = spark.read.csv(data_paths['customer_list'], header=True, sep=";")

# Doc file don hang
print("2. Dang doc du lieu don hang...")
df_orders = spark.read.csv(data_paths['order'], header=True, sep=";")

# Ket noi hai DataFrame theo Customer_Trx_ID
# Moi don hang (Orders) co Customer_Trx_ID
# Moi khach hang (Customers) co Customer_Trx_ID va Customer_Country
print("3. Dang ket noi du lieu don hang va khach hang...")

# Cach join:
# - on="Customer_Trx_ID": Join theo cot nay
# - how="inner": Chi lay cac ban ghi co trong ca 2 bang
df_joined = df_orders.join(df_customers, on="Customer_Trx_ID", how="inner")

# Nhom theo quoc gia va dem so don hang
print("4. Dang nhom va dem so don hang theo quoc gia...")

# groupBy("Customer_Country"): Nhom theo quoc gia
# agg(count("Order_ID")): Dem so Order_ID trong moi nhom
# alias("Total_Orders"): Dat ten cho cot ket qua
# orderBy(desc("Total_Orders")): Sap xep giam dan theo so don hang
df_result = df_joined.groupBy("Customer_Country") \
                     .agg(count("Order_ID").alias("Total_Orders")) \
                     .orderBy(desc("Total_Orders"))

# Lay ket qua (top 10 quoc gia)
print("5. Dang lay ket qua...\n")
top_countries = df_result.collect()

# Ghi ket qua ra file
output_path = './result/result_bai3.txt'

with open(output_path, 'w', encoding='utf-8') as f:
    f.write("=" * 60 + "\n")
    f.write("PHAN TICH SO LUONG DON HANG THEO QUOC GIA\n")
    f.write("=" * 60 + "\n\n")
    
    # Ghi toan bo ket qua
    for row in top_countries:
        country = row['Customer_Country']
        total = row['Total_Orders']
        f.write(f"{country}: {total:,} don hang\n")
    
    f.write("\n" + "=" * 60 + "\n")

# In top 10 quoc gia ra console
print("=" * 60)
print("TOP 10 QUOC GIA CO NHIEU DON HANG NHAT:")
print("=" * 60)
for i, row in enumerate(top_countries[:10], 1):
    country = row['Customer_Country']
    total = row['Total_Orders']
    print(f"{i:2}. {country:20} - {total:,} don hang")
print("=" * 60)

# Dong Spark Session
spark.stop()

print(f"\nHoan thanh! Ket qua da duoc luu vao: {output_path}")
