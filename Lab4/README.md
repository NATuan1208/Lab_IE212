# Lab 4 - Streaming Video Processing with TCP Sockets and Apache Spark

## Thong Tin
- **Mon hoc**: IE212 - Big Data
- **Ho ten sinh vien**: Nguyen Anh Tuan
- **MSSV**: 23521716

---

## Mo Ta Bai Toan

Xay dung he thong xu ly video streaming su dung:
- **TCP Sockets**: Truyen du lieu video tu Camera Server den Spark Processor
- **Apache Spark Structured Streaming**: Xu ly luan du lieu video
- **MediaPipe + TFLite**: Xoa background tu video frames

### Kien Truc He Thong

```
+------------------+         TCP Socket         +--------------------+
|   Camera Server  | ----------------------->  |   Spark Processor   |
|    (Producer)    |      Port 9999            |     (Consumer)      |
+------------------+                           +--------------------+
        |                                               |
        v                                               v
  [Webcam/Video]                              [Background Removal]
        |                                               |
        v                                               v
  [Resize 640x480]                            [Save Processed Frames]
        |                                               |
        v                                               v
  [Encode Base64]                              [./output_frames/]
        |
        v
  [JSON + Newline]
```

---

## Cau Truc Thu Muc

```
Lab4/
├── camera_server.py           # Module 1: Camera Server (Producer)
├── spark_processor.py         # Module 2: Spark Processor (Consumer)
├── background_remover.py      # Reference: Background removal logic
├── tcp_example.py             # Reference: TCP connection example
├── selfie_segmenter.tflite    # TFLite model for segmentation
├── run_camera_server.ps1      # Script chay Camera Server
├── run_spark_processor.ps1    # Script chay Spark Processor
├── output_frames/             # Output: Processed frames (auto-created)
├── checkpoint/                # Spark checkpoint (auto-created)
└── README.md                  # Tai lieu nay
```

---

## Yeu Cau He Thong

### Phan Mem
- **Java**: JDK 17 tro len
- **Python**: 3.11
- **PySpark**: 3.4.1
- **OpenCV**: opencv-python
- **MediaPipe**: mediapipe

### Cai Dat Dependencies

```bash
# Kich hoat virtual environment
cd Lab
.\venv\Scripts\Activate.ps1

# Cai dat cac thu vien can thiet
pip install pyspark==3.4.1
pip install opencv-python
pip install mediapipe
pip install numpy
```

---

## Cach Chay He Thong

### Buoc 1: Chay Camera Server (Terminal 1)

```powershell
cd Lab4
.\run_camera_server.ps1
```

Server se:
1. Khoi dong TCP server tren port 9999
2. Cho ket noi tu Spark Processor
3. Bat dau stream video khi co ket noi

### Buoc 2: Chay Spark Processor (Terminal 2)

```powershell
cd Lab4
.\run_spark_processor.ps1
```

Processor se:
1. Khoi tao Spark Session
2. Ket noi den Camera Server qua TCP
3. Nhan va xu ly frames
4. Luu ket qua vao `./output_frames/`

### Thu Tu Chay Quan Trong

1. **Chay Camera Server TRUOC** (Terminal 1)
2. **Cho den khi thay "Waiting for connection..."**
3. **Chay Spark Processor** (Terminal 2)
4. **Camera Server se bat dau stream khi Spark ket noi**

---

## Chi Tiet Ky Thuat

### Module 1: Camera Server (Producer)

**File**: `camera_server.py`

**Chuc nang chinh**:

1. **Video Capture**: Su dung OpenCV de doc tu webcam hoac file video
   ```python
   cap = cv2.VideoCapture(0)  # 0 = webcam
   ```

2. **Resize Frame**: Giam kich thuoc de toi uu bang thong
   ```python
   frame = cv2.resize(frame, (640, 480))
   ```

3. **Encode Base64**: Chuyen doi frame thanh string de truyen qua mang
   ```python
   _, buffer = cv2.imencode('.jpg', frame)
   base64_string = base64.b64encode(buffer).decode('utf-8')
   ```

4. **JSON Payload**: Dong goi du lieu voi metadata
   ```python
   payload = {
       "timestamp": time.time(),
       "frame_width": 640,
       "frame_height": 480,
       "frame_data": base64_string
   }
   ```

5. **TCP Send**: Gui qua socket voi delimiter `\n`
   ```python
   connection.send((json.dumps(payload) + "\n").encode())
   ```

### Module 2: Spark Processor (Consumer)

**File**: `spark_processor.py`

**Chuc nang chinh**:

1. **Socket Stream**: Doc du lieu tu TCP port
   ```python
   raw_stream = spark.readStream \
       .format("socket") \
       .option("host", "localhost") \
       .option("port", 9999) \
       .load()
   ```

2. **Parse JSON**: Giai ma JSON payload
   ```python
   parsed_stream = raw_stream \
       .select(from_json(col("value"), frame_schema).alias("data")) \
       .select("data.*")
   ```

3. **UDF Background Removal**: Xu ly frame trong Spark context
   ```python
   @udf(StringType())
   def process_frame_udf(base64_data, timestamp, ...):
       # Decode Base64 -> Image
       # Apply MediaPipe segmentation
       # Save processed frame
       return output_path
   ```

4. **Lazy Loading Model**: Tranh loi Serialization
   ```python
   _segmenter = None
   def get_segmenter(model_path):
       global _segmenter
       if _segmenter is None:
           _segmenter = vision.ImageSegmenter.create_from_options(...)
       return _segmenter
   ```

5. **Write Stream**: Xuat ket qua
   ```python
   query = processed_stream.writeStream \
       .outputMode("append") \
       .format("console") \
       .start()
   ```

---

## Xu Ly Loi

### 1. BrokenPipeError (Camera Server)
**Nguyen nhan**: Spark Processor ngat ket noi
**Xu ly**: Bat exception va dung stream an toan

### 2. Empty Packets (Spark Processor)
**Nguyen nhan**: Frame data bi NULL hoac rong
**Xu ly**: Filter truoc khi xu ly
```python
.filter(col("frame_data").isNotNull())
```

### 3. Model Serialization Error
**Nguyen nhan**: MediaPipe model khong the pickle
**Xu ly**: Lazy loading trong UDF (chi load 1 lan per executor)

### 4. Connection Refused
**Nguyen nhan**: Camera Server chua chay
**Xu ly**: Chay Camera Server truoc, cho "Waiting for connection..."

---

## Ket Qua Mong Doi

### Console Output - Camera Server
```
============================================================
CAMERA SERVER - Video Streaming Producer
============================================================
[Camera Server] Da khoi dong tai localhost:9999
[Camera Server] Dang cho Spark consumer ket noi...
[Camera Server] Da ket noi voi consumer: ('127.0.0.1', 62174)
[Camera Server] Bat dau stream video tai 10 FPS...
[Camera Server] Da gui 10 frames
[Camera Server] Da gui 20 frames
...
```

### Console Output - Spark Processor
```
============================================================
SPARK PROCESSOR - Video Stream Consumer
============================================================
[Spark] Dang khoi tao Spark session...
[Spark] Dang ket noi den TCP stream tai localhost:9999...
[Spark] Vui long khoi dong Camera Server truoc!
[Spark] Da bat dau xu ly stream!
[Spark] Cac frame da xu ly se duoc luu tai: ./output_frames
[Spark] Nhan Ctrl+C de dung

INFO: Created TensorFlow Lite XNNPACK delegate for CPU.
W0000 00:00:1767668131.630157 19000 inference_feedback_manager.cc:121] Feedback manager requires a model...

-------------------------------------------
Batch: 1
-------------------------------------------
+--------------------+---------------------------------------+
|timestamp           |output_path                            |
+--------------------+---------------------------------------+
|1.7676681283084435E9|./output_frames\frame_1767668128308.jpg|
|1.7676681295113678E9|./output_frames\frame_1767668129511.jpg|
|1.7676681284394026E9|./output_frames\frame_1767668128439.jpg|
|1.767668129572108E9 |./output_frames\frame_1767668129572.jpg|
...
+--------------------+---------------------------------------+
```

### Ket Qua Xu Ly

| Metric | Gia tri |
|--------|---------|
| Tong frames xu ly | **155 frames** |
| Dung luong trung binh/frame | ~18-19 KB |
| Tong dung luong output | ~4.6 MB |
| TFLite model | Chay thanh cong (XNNPACK delegate) |
| Background removal | Thanh cong (mau xam 192,192,192) |
| Frame save | Thanh cong (JPG format) |

### Output Files
```
output_frames/
├── frame_1767668128308.jpg   # 18.8 KB - Background removed
├── frame_1767668128439.jpg   # 18.5 KB
├── frame_1767668128445.jpg   # 18.5 KB
├── frame_1767668128548.jpg   # 18.5 KB
├── frame_1767668128651.jpg   # 18.9 KB
└── ... (151 files more)
```

### Ket Luan

✅ **Pipeline hoat dong toan phan thanh cong**:
- TCP Socket communication giua Camera Server va Spark Processor
- Spark Structured Streaming nhan va xu ly real-time video frames
- MediaPipe TFLite model xoa background tu frames
- Ket qua frames duoc luu ra file JPEG

---

## Kien Thuc Ap Dung

1. **Spark Structured Streaming**: Xu ly du lieu streaming voi DataFrame API
2. **TCP Socket Communication**: Truyen du lieu giua cac process
3. **Base64 Encoding**: Chuyen doi binary data sang text de truyen qua JSON
4. **UDF (User Defined Function)**: Mo rong chuc nang Spark voi Python code
5. **Lazy Loading**: Toi uu hieu suat bang cach load model 1 lan
6. **MediaPipe Image Segmentation**: Xu ly AI de xoa background

---

## Ghi Chu

- Su dung `local[*]` mode de chay Spark tren may local
- FPS gioi han o muc 10 de tranh qua tai mang va xu ly
- Checkpoint directory can thiet cho Spark Streaming
- Model TFLite phai nam trong cung thu muc voi spark_processor.py

---

## Tham Khao

- [Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [MediaPipe Image Segmentation](https://developers.google.com/mediapipe/solutions/vision/image_segmenter)
- [Python Socket Programming](https://docs.python.org/3/library/socket.html)
