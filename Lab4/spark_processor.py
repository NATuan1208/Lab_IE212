"""
Spark Processing Server (Consumer) - Module 2
-----------------------------------------------
Xu ly video stream su dung Apache Spark Structured Streaming.
- Lang nghe TCP socket de nhan cac frame
- Ap dung xoa background su dung TFLite model
- Luu cac frame da xu ly vao thu muc output
"""

import os
import sys
import base64
import time
import json
import numpy as np
import cv2

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType


class SparkProcessorConfig:
    """Cau hinh cho Spark Processor"""
    HOST = "localhost"
    PORT = 9999
    OUTPUT_DIR = "./output_frames"
    CHECKPOINT_DIR = "./checkpoint"
    MODEL_PATH = "./selfie_segmenter.tflite"
    
    # Cau hinh xoa background
    BG_COLOR = (192, 192, 192)  # Mau nen xam
    MASK_THRESHOLD = 0.2


# Bien global de lazy loading segmenter model
_segmenter = None


def get_segmenter(model_path):
    """
    Lazy load MediaPipe segmenter model.
    Dam bao model chi duoc load mot lan moi executor.
    
    Tham so:
        model_path: Duong dan den file TFLite model
    Tra ve:
        Instance cua ImageSegmenter
    """
    global _segmenter
    
    if _segmenter is None:
        import mediapipe as mp
        from mediapipe.tasks import python
        from mediapipe.tasks.python import vision
        
        base_options = python.BaseOptions(model_asset_path=model_path)
        options = vision.ImageSegmenterOptions(
            base_options=base_options, 
            output_category_mask=True
        )
        _segmenter = vision.ImageSegmenter.create_from_options(options)
        
    return _segmenter


def remove_background_from_image(image_data, model_path, bg_color, threshold):
    """
    Xoa background khoi anh su dung MediaPipe segmentation.
    
    Tham so:
        image_data: numpy array cua anh
        model_path: duong dan den TFLite model
        bg_color: tuple mau nen (R, G, B)
        threshold: nguong segmentation
    Tra ve:
        Anh da xu ly voi background bi xoa
    """
    import mediapipe as mp
    
    segmenter = get_segmenter(model_path)
    
    # Tao MediaPipe image
    mp_image = mp.Image(image_format=mp.ImageFormat.SRGB, data=image_data)
    
    # Thuc hien segmentation
    segmentation_result = segmenter.segment(mp_image)
    category_mask = segmentation_result.category_mask
    
    # Tao anh output
    image_np = mp_image.numpy_view()
    bg_image = np.zeros(image_np.shape, dtype=np.uint8)
    bg_image[:] = bg_color
    
    # Lay mask va xu ly shape
    mask = category_mask.numpy_view()
    
    # Dam bao mask co shape (H, W) - neu co them chieu thi squeeze
    if mask.ndim > 2:
        mask = mask.squeeze()
    
    # Tao condition mask voi shape (H, W, 3)
    condition = np.stack([mask > threshold] * 3, axis=-1)
    
    # Ap dung mask
    output_frame = np.where(condition, bg_image, image_np)
    
    return output_frame


def process_frame_udf(base64_data, timestamp, frame_width, frame_height):
    """
    UDF de xu ly mot frame don le.
    Giai ma Base64, ap dung xoa background, luu ra file.
    
    Tham so:
        base64_data: Chuoi anh da ma hoa Base64
        timestamp: Timestamp cua frame
        frame_width: Chieu rong frame goc
        frame_height: Chieu cao frame goc
    Tra ve:
        Duong dan den frame da xu ly va luu
    """
    if base64_data is None or len(base64_data) == 0:
        return "error: du lieu frame rong"
    
    try:
        # Giai ma Base64 sang anh
        img_bytes = base64.b64decode(base64_data)
        img_array = np.frombuffer(img_bytes, dtype=np.uint8)
        frame = cv2.imdecode(img_array, cv2.IMREAD_COLOR)
        
        if frame is None:
            return "error: khong the giai ma anh"
        
        # Chuyen BGR sang RGB cho MediaPipe
        frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        
        # Ap dung xoa background
        processed_frame = remove_background_from_image(
            frame_rgb,
            SparkProcessorConfig.MODEL_PATH,
            SparkProcessorConfig.BG_COLOR,
            SparkProcessorConfig.MASK_THRESHOLD
        )
        
        # Chuyen lai sang BGR de luu
        processed_bgr = cv2.cvtColor(processed_frame, cv2.COLOR_RGB2BGR)
        
        # Tao thu muc output neu chua ton tai
        os.makedirs(SparkProcessorConfig.OUTPUT_DIR, exist_ok=True)
        
        # Tao ten file voi timestamp
        filename = f"frame_{int(timestamp * 1000)}.jpg"
        output_path = os.path.join(SparkProcessorConfig.OUTPUT_DIR, filename)
        
        # Luu frame da xu ly
        cv2.imwrite(output_path, processed_bgr)
        
        return output_path
        
    except Exception as e:
        return f"error: {str(e)}"


def create_spark_session():
    """Tao va cau hinh Spark session"""
    import os
    
    # Tao thu muc temp cho Spark de tranh loi Hadoop tren Windows
    temp_dir = os.path.abspath("./spark_temp")
    os.makedirs(temp_dir, exist_ok=True)
    
    spark = SparkSession.builder \
        .appName("VideoStreamProcessor") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.local.dir", temp_dir) \
        .getOrCreate()
    
    # Dat log level de giam output
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def main():
    """Diem vao chinh cua Spark processor"""
    print("=" * 60)
    print("SPARK PROCESSOR - Video Stream Consumer")
    print("=" * 60)
    
    # Kiem tra file model ton tai
    if not os.path.exists(SparkProcessorConfig.MODEL_PATH):
        print(f"[LOI] Khong tim thay file model: {SparkProcessorConfig.MODEL_PATH}")
        print("Vui long dam bao selfie_segmenter.tflite nam trong thu muc hien tai")
        sys.exit(1)
    
    # Tao cac thu muc output
    os.makedirs(SparkProcessorConfig.OUTPUT_DIR, exist_ok=True)
    os.makedirs(SparkProcessorConfig.CHECKPOINT_DIR, exist_ok=True)
    
    # Tao Spark session
    print("[Spark] Dang khoi tao Spark session...")
    spark = create_spark_session()
    
    # Dinh nghia schema cho du lieu JSON dau vao
    frame_schema = StructType([
        StructField("timestamp", DoubleType(), True),
        StructField("frame_width", IntegerType(), True),
        StructField("frame_height", IntegerType(), True),
        StructField("frame_data", StringType(), True)
    ])
    
    # Dang ky UDF de xu ly frame
    process_frame = udf(process_frame_udf, StringType())
    
    print(f"[Spark] Dang ket noi den TCP stream tai {SparkProcessorConfig.HOST}:{SparkProcessorConfig.PORT}...")
    print("[Spark] Vui long khoi dong Camera Server truoc!")
    
    try:
        # Doc tu TCP socket stream
        raw_stream = spark.readStream \
            .format("socket") \
            .option("host", SparkProcessorConfig.HOST) \
            .option("port", SparkProcessorConfig.PORT) \
            .load()
        
        # Parse du lieu JSON
        parsed_stream = raw_stream \
            .select(from_json(col("value"), frame_schema).alias("data")) \
            .select("data.*")
        
        # Ap dung UDF xoa background
        processed_stream = parsed_stream \
            .filter(col("frame_data").isNotNull()) \
            .withColumn(
                "output_path",
                process_frame(
                    col("frame_data"),
                    col("timestamp"),
                    col("frame_width"),
                    col("frame_height")
                )
            ) \
            .select("timestamp", "output_path")
        
        # Tao duong dan checkpoint tuyet doi voi timestamp de tranh xung dot
        import uuid
        checkpoint_path = os.path.abspath(
            os.path.join(SparkProcessorConfig.CHECKPOINT_DIR, str(uuid.uuid4())[:8])
        )
        
        # Ghi stream output - su dung console de debug
        query = processed_stream \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .option("checkpointLocation", checkpoint_path) \
            .trigger(processingTime="2 seconds") \
            .start()
        
        print("[Spark] Da bat dau xu ly stream!")
        print(f"[Spark] Cac frame da xu ly se duoc luu tai: {SparkProcessorConfig.OUTPUT_DIR}")
        print("[Spark] Nhan Ctrl+C de dung")
        
        # Cho den khi ket thuc
        query.awaitTermination()
        
    except Exception as e:
        print(f"[Spark] Loi: {e}")
        raise
        
    finally:
        spark.stop()
        print("[Spark] Da dong session")


if __name__ == "__main__":
    main()
