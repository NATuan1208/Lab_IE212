"""
Bai 8: Tinh toan hieu suat giao hang
-------------------------------------
Muc tieu:
- Tinh hieu so giua ngay giao hang thuc te va ngay du kien
- Danh gia hieu suat giao hang (som, dung han, tre)
- Thong ke ty le giao hang dung han

Kien thuc su dung:
- join(): Ket noi Orders va Order_Items
- datediff(): Tinh so ngay chenh lech giua 2 ngay
- when().otherwise(): Dieu kien phan loai
- Xu ly NULL values
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff, when, count, avg, round

# Khoi tao Spark Session
spark = SparkSession.builder.appName("Lab3_Bai8").getOrCreate()

# Dinh nghia duong dan den cac file du lieu
data_paths = {
    "customer_list": './data/Customer_List.csv',
    "order_item": './data/Order_Items.csv',
    "order_review": './data/Order_Reviews.csv',
    "order": './data/Orders.csv',
    "product": './data/Products.csv'
}

print("Bat dau phan tich hieu suat giao hang...\n")

# Doc file don hang
print("1. Dang doc du lieu don hang va chi tiet don hang...")
df_orders = spark.read.csv(data_paths['order'], header=True, sep=";")
df_order_items = spark.read.csv(data_paths['order_item'], header=True, sep=";")

# Ket noi hai bang theo Order_ID
print("2. Dang ket noi du lieu...")
# Chi lay cac cot can thiet de tiet kiem bo nho
df_joined = df_orders.select("Order_ID", "Order_Delivered_Carrier_Date") \
                     .join(df_order_items.select("Order_ID", "Shipping_Limit_Date"), 
                           on="Order_ID", 
                           how="inner")

# Loc chi lay cac don hang da giao (co ngay giao hang thuc te)
df_delivered = df_joined.filter(
    col("Order_Delivered_Carrier_Date").isNotNull() & 
    col("Shipping_Limit_Date").isNotNull()
)

# Tinh hieu so ngay
print("3. Dang tinh hieu so ngay giao hang...")
# datediff(ngay1, ngay2): Tra ve so ngay giua 2 ngay (ngay1 - ngay2)
# Neu > 0: giao tre
# Neu = 0: giao dung han
# Neu < 0: giao som
df_delivery_performance = df_delivered.withColumn(
    "Days_Difference",
    datediff(col("Order_Delivered_Carrier_Date"), col("Shipping_Limit_Date"))
)

# Phan loai hieu suat giao hang
df_delivery_performance = df_delivery_performance.withColumn(
    "Delivery_Status",
    when(col("Days_Difference") > 0, "Giao tre")
    .when(col("Days_Difference") < 0, "Giao som")
    .otherwise("Dung han")
)

# Thong ke tong quan
print("4. Dang thong ke hieu suat giao hang...\n")

# Dem so luong theo tung loai
df_status_count = df_delivery_performance.groupBy("Delivery_Status") \
                                         .agg(count("*").alias("Total_Orders")) \
                                         .orderBy("Delivery_Status")

# Tinh so ngay tre trung binh (chi xet cac don giao tre)
avg_late_days = df_delivery_performance.filter(col("Days_Difference") > 0) \
                                       .agg(avg("Days_Difference")).collect()[0][0]

# Tinh so ngay som trung binh (chi xet cac don giao som)
avg_early_days = df_delivery_performance.filter(col("Days_Difference") < 0) \
                                        .agg(avg("Days_Difference")).collect()[0][0]

# Tong so don hang
total_orders = df_delivery_performance.count()

# Lay ket qua
status_results = df_status_count.collect()

# Tinh ty le phan tram
stats = {}
for row in status_results:
    status = row['Delivery_Status']
    total = row['Total_Orders']
    percentage = (total / total_orders) * 100
    stats[status] = {'total': total, 'percentage': percentage}

# Ghi ket qua ra file
output_path = './result/result_bai8.txt'

with open(output_path, 'w', encoding='utf-8') as f:
    f.write("=" * 70 + "\n")
    f.write("PHAN TICH HIEU SUAT GIAO HANG\n")
    f.write("=" * 70 + "\n\n")
    
    f.write(f"Tong so don hang da giao: {total_orders:,}\n\n")
    
    f.write("PHAN BO THEO TRANG THAI GIAO HANG:\n")
    f.write("-" * 70 + "\n")
    
    for row in status_results:
        status = row['Delivery_Status']
        total = row['Total_Orders']
        percentage = (total / total_orders) * 100
        bar = '#' * int(percentage / 2)
        f.write(f"{status:15} : {total:6,} don hang ({percentage:5.2f}%) {bar}\n")
    
    f.write("\n" + "-" * 70 + "\n")
    
    # Thong tin chi tiet
    f.write("\nCHI TIET HIEU SUAT:\n")
    f.write("-" * 70 + "\n")
    
    if avg_late_days:
        f.write(f"So ngay tre trung binh: {abs(avg_late_days):.2f} ngay\n")
    
    if avg_early_days:
        f.write(f"So ngay giao som trung binh: {abs(avg_early_days):.2f} ngay\n")
    
    # Danh gia
    f.write("\n" + "=" * 70 + "\n")
    f.write("DANH GIA:\n")
    f.write("=" * 70 + "\n")
    
    if 'Dung han' in stats:
        on_time_rate = stats['Dung han']['percentage']
        if on_time_rate >= 90:
            f.write("Hieu suat giao hang: XUAT SAC (>= 90%)\n")
        elif on_time_rate >= 80:
            f.write("Hieu suat giao hang: TOT (>= 80%)\n")
        elif on_time_rate >= 70:
            f.write("Hieu suat giao hang: TRUNG BINH (>= 70%)\n")
        else:
            f.write("Hieu suat giao hang: CAN CAI THIEN (< 70%)\n")
    
    f.write("\n" + "=" * 70 + "\n")

# In ket qua ra console
print("=" * 70)
print("KET QUA PHAN TICH HIEU SUAT GIAO HANG")
print("=" * 70)
print(f"\nTong so don hang da giao: {total_orders:,}\n")

print("Phan bo theo trang thai:")
print("-" * 70)
for row in status_results:
    status = row['Delivery_Status']
    total = row['Total_Orders']
    percentage = (total / total_orders) * 100
    bar = '#' * int(percentage / 2)
    print(f"{status:15} : {total:6,} don hang ({percentage:5.2f}%) {bar}")

print("\n" + "-" * 70)
print("Chi tiet:")
if avg_late_days:
    print(f"  - So ngay tre trung binh: {abs(avg_late_days):.2f} ngay")
if avg_early_days:
    print(f"  - So ngay giao som trung binh: {abs(avg_early_days):.2f} ngay")

print("=" * 70)

# Dong Spark Session
spark.stop()

print(f"\nHoan thanh! Ket qua da duoc luu vao: {output_path}")
