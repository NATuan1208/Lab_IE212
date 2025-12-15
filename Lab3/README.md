# Bao Cao Lab 3 - Phan Tich Du Lieu Thuong Mai Dien Tu Su Dung Spark DataFrame

## Thong Tin
- **Mon hoc**: IE212 - Big Data
- **Ho ten sinh vien**: Nguyen Anh Tuan
- **MSSV**: 23521716

---

## Moi Truong Cai Dat

### Yeu Cau
- **Java**: JDK 17 tro len
- **Python**: 3.11
- **Spark**: 3.4.1
- **OS**: Windows 10/11 (voi PowerShell 5.1)

### Cai Dat PySpark

1. **Tao Python Virtual Environment** (o cap Lab):
   ```bash
   cd Lab
   python -m venv venv
   .\venv\Scripts\Activate.ps1
   ```

2. **Cai dat PySpark**:
   ```bash
   pip install pyspark==3.4.1
   ```

---

## Cau Truc Thu Muc

```
Lab3/
├── data/
│   ├── Customer_List.csv
│   ├── Orders.csv
│   ├── Order_Items.csv
│   ├── Order_Reviews.csv
│   └── Products.csv
├── result/
│   ├── result_bai1.txt
│   ├── result_bai2.txt
│   ├── result_bai3.txt
│   ├── result_bai4.txt
│   ├── result_bai5.txt
│   └── result_bai8.txt
├── Bai1.py              # Doc va hien thi schema
├── Bai2.py              # Thong ke don hang, khach hang, nguoi ban
├── Bai3.py              # Phan tich don hang theo quoc gia
├── Bai4.py              # Phan tich don hang theo nam, thang
├── Bai5.py              # Thong ke diem danh gia
├── Bai8.py              # Phan tich hieu suat giao hang
├── run_bai1.ps1         # Script chay Bai1
├── run_bai2.ps1         # Script chay Bai2
├── run_bai3.ps1         # Script chay Bai3
├── run_bai4.ps1         # Script chay Bai4
├── run_bai5.ps1         # Script chay Bai5
├── run_bai8.ps1         # Script chay Bai8
└── README.md            # Tai lieu nay
```

---

## Format Du Lieu

### Customer_List.csv
- **Schema**: `Customer_Trx_ID;Subscriber_ID;Subscribe_Date;First_Order_Date;Customer_Postal_Code;Customer_City;Customer_Country;Customer_Country_Code;Age;Gender`
- **Delimiter**: `;` (dau cham phay)
- **Tong so**: 102,727 khach hang

### Orders.csv
- **Schema**: `Order_ID;Customer_Trx_ID;Order_Status;Order_Purchase_Timestamp;Order_Approved_At;Order_Delivered_Carrier_Date;Order_Delivered_Customer_Date;Order_Estimated_Delivery_Date`
- **Delimiter**: `;`
- **Tong so**: 99,441 don hang

### Order_Items.csv
- **Schema**: `Order_ID;Order_Item_ID;Product_ID;Seller_ID;Shipping_Limit_Date;Price;Freight_Value`
- **Delimiter**: `;`
- **Tong so**: 112,651 san pham trong don hang

### Order_Reviews.csv
- **Schema**: `Review_ID;Order_ID;Review_Score;Review_Comment_Title_En;Review_Comment_Message_En;Review_Creation_Date;Review_Answer_Timestamp`
- **Delimiter**: `;`
- **Tong so**: 99,270 danh gia

### Products.csv
- **Schema**: `Product_ID;Product_Category_Name;Product_Weight_Gr;Product_Length_Cm;Product_Height_Cm;Product_Width_Cm`
- **Delimiter**: `;`
- **Tong so**: 32,951 san pham

---

## Cach Chay Cac Bai Tap

### Chay tung bai (Windows PowerShell)

```bash
cd Lab3

# Bai 1: Doc va hien thi schema cua cac file CSV
.\run_bai1.ps1

# Bai 2: Thong ke tong so don hang, khach hang, nguoi ban
.\run_bai2.ps1

# Bai 3: Phan tich so luong don hang theo quoc gia
.\run_bai3.ps1

# Bai 4: Phan tich so luong don hang theo nam, thang
.\run_bai4.ps1

# Bai 5: Thong ke diem danh gia cua khach hang
.\run_bai5.ps1

# Bai 8: Phan tich hieu suat giao hang
.\run_bai8.ps1
```

---

## Ket Qua Tung Bai

### Bai 1: Doc va Hien Thi Schema Cua Cac File CSV

**Muc tieu**: Doc cac file CSV va su dung `inferSchema=True` de Spark tu dong suy ra kieu du lieu

**Ket qua**: 
```
Customer_List.csv
  - Customer_Trx_ID: string
  - Subscribe_Date: date
  - Age: int
  - Gender: string
  ...

Order_Items.csv
  - Order_ID: string
  - Price: double
  - Freight_Value: double
  ...
```

**Kien thuc su dung**: `spark.read.csv()`, `inferSchema`, `schema.fields`

---

### Bai 2: Thong Ke Tong So Don Hang, Khach Hang va Nguoi Ban

**Muc tieu**: Dem tong so don hang, khach hang va nguoi ban trong he thong

**Ket qua**:
```
Tong so don hang: 99,441
Tong so khach hang: 102,727
Tong so nguoi ban: 3,095
```

**Kien thuc su dung**: `count()`, `distinct()`, `countDistinct()`, `agg()`

---

### Bai 3: Phan Tich So Luong Don Hang Theo Quoc Gia

**Muc tieu**: Thong ke so don hang theo tung quoc gia, sap xep giam dan

**Top 5 quoc gia**:
```
1. Germany              - 41,754 don hang
2. France               - 12,848 don hang
3. Netherlands          - 11,629 don hang
4. Belgium              - 5,464 don hang
5. Austria              - 5,043 don hang
```

**Kien thuc su dung**: `join()`, `groupBy()`, `count()`, `orderBy(desc())`

---

### Bai 4: Phan Tich So Luong Don Hang Theo Nam, Thang

**Muc tieu**: Nhom don hang theo nam va thang, sap xep nam tang dan, thang giam dan

**Ket qua mau**:
```
NAM 2023:
  Thang 12: 5,673 don hang
  Thang 11: 7,544 don hang
  Thang 10: 4,631 don hang
  ...

NAM 2024:
  Thang 8: 6,512 don hang
  ...
```

**Kien thuc su dung**: `year()`, `month()`, `withColumn()`, `groupBy()`, `orderBy(asc(), desc())`

---

### Bai 5: Thong Ke Diem Danh Gia Cua Khach Hang

**Muc tieu**: Tinh diem trung binh va phan bo danh gia, xu ly NULL values

**Ket qua**:
```
Diem trung binh: 4.0864

Phan bo theo muc:
1 sao: 11,424 danh gia (11.51%)
2 sao:  3,151 danh gia ( 3.18%)
3 sao:  8,179 danh gia ( 8.24%)
4 sao: 19,141 danh gia (19.29%)
5 sao: 57,328 danh gia (57.78%)

Da loc: 47 danh gia khong hop le
```

**Kien thuc su dung**: `try_cast()`, `filter()`, `isNotNull()`, `avg()`, `expr()`

---

### Bai 8: Phan Tich Hieu Suat Giao Hang

**Muc tieu**: Tinh hieu so giua ngay giao hang thuc te va du kien, danh gia hieu suat

**Ket qua**:
```
Tong so don hang da giao: 111,456

Phan bo theo trang thai:
Dung han        :  7,599 don hang ( 6.82%)
Giao som        : 97,187 don hang (87.20%)
Giao tre        :  6,670 don hang ( 5.98%)

So ngay tre trung binh: 4.67 ngay
So ngay giao som trung binh: 4.26 ngay
```

**Nhan xet**: 
- 87.20% don hang giao som hon du kien → Hieu suat tot
- Chi 5.98% don hang giao tre → Can duy tri va cai thien
- He thong giao hang hoat dong hieu qua

**Kien thuc su dung**: `datediff()`, `when().otherwise()`, `join()`, `filter()`

---

## Kien Thuc Rut Ra

1. **Spark DataFrame API** manh me hon RDD cho xu ly du lieu co cau truc
2. **inferSchema=True** giup tu dong suy ra kieu du lieu, tiet kiem thoi gian
3. **join()** operations rat quan trong khi lam viec voi nhieu bang
4. **Xu ly NULL values** la bat buoc truoc khi thuc hien phan tich
5. **Aggregate functions** (`count`, `avg`, `sum`) la co so cua phan tich du lieu
6. **Date functions** (`year`, `month`, `datediff`) giup phan tich thoi gian
7. **when().otherwise()** de tao cot dieu kien (tuong tu CASE WHEN trong SQL)

---

## Cac Van De Thuong Gap va Cach Giai Quyet

### 1. Loi "Python worker crash" tren Windows
**Nguyen nhan**: Duong dan chua khoang trang ("HK1 2025 - 2026")

**Giai phap**: Set bien moi truong trong script `.ps1`:
```powershell
$env:PYSPARK_PYTHON = "...\venv\Scripts\python.exe"
$env:PYSPARK_DRIVER_PYTHON = "...\venv\Scripts\python.exe"
```

### 2. Loi "HADOOP_HOME not set"
**Nguyen nhan**: Spark thiet ke cho Linux, thieu winutils.exe tren Windows

**Giai phap**: Khong can xu ly, chi la warning, khong anh huong ket qua

### 3. Delimiter sai khi doc CSV
**Nguyen nhan**: Du lieu dung `;` thay vi `,`

**Giai phap**: Chi dinh `sep=";"` khi doc file

### 4. Review_Score la string thay vi integer
**Nguyen nhan**: Du lieu co the chua gia tri khong hop le

**Giai phap**: Dung `try_cast()` thay vi `cast()` de tranh loi

---

## Ghi Chu

- Tren Windows, can thiet lap bien moi truong `PYSPARK_PYTHON` va `PYSPARK_DRIVER_PYTHON`
- Spark master chay o che do `local[*]` de su dung toan bo CPU cores
- Nen loc du lieu (loai NULL, ngoai le) truoc khi thuc hien phan tich
- Su dung `.distinct()` khi dem de tranh duplicate
- Voi du lieu lon, can toi uu join operations de tang hieu suat

---

## Tham Khao

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)
- [Spark SQL Functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)
