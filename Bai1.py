from pyspark.sql import SparkSession


def parse_rating(line):
	"""
	Hàm parse dòng đánh giá thành tuple (movieId, (rating, 1)).
	Định dạng: userId,movieId,rating,timestamp
	Trả về: (movieId, (rating, 1)) để dùng reduceByKey tính tổng và đếm.
	"""
	try:
		parts = line.strip().split(",")
		if len(parts) < 3:
			return None
		movie_id = int(parts[1])
		rating = float(parts[2])
		return (movie_id, (rating, 1))
	except Exception:
		return None


def parse_movie(line):
	"""
	Hàm parse dòng phim thành tuple (movieId, title).
	Định dạng: movieId,title,genres
	Trả về: (movieId, title)
	"""
	try:
		parts = line.strip().split(",")
		if len(parts) < 2:
			return None
		movie_id = int(parts[0])
		title = parts[1]
		return (movie_id, title)
	except Exception:
		return None


def main():
	"""
	Hàm chính: Sử dụng PySpark RDD để xử lý dữ liệu.
	1. Đọc file thành RDD
	2. Parse ratings, dùng reduceByKey để tính tổng/đếm (RDD operation)
	3. Tính trung bình bằng mapValues (RDD operation)
	4. Ghép với movies bằng leftOuterJoin (RDD operation)
	5. Collect về local để tránh lỗi worker trên Windows
	6. Xử lý và in kết quả trên Python
	"""
	# Khởi tạo Spark
	spark = SparkSession.builder \
		.appName("Bai1_RDD_Ratings") \
		.master("local[*]") \
		.getOrCreate()
	
	sc = spark.sparkContext
	print("=== Bài 1: Tính điểm đánh giá phim (dùng RDD) ===")
	print(f"Phiên bản Spark: {spark.version}\n")

	try:
		# Bước 1: Đọc file ratings thành RDD
		rdd1 = sc.textFile("ratings_1.txt")
		rdd2 = sc.textFile("ratings_2.txt")
		
		# Bước 2: Parse và hợp nhất
		ratings_rdd = rdd1.union(rdd2) \
			.map(parse_rating) \
			.filter(lambda x: x is not None)
		
		# Bước 3: Tính tổng/đếm bằng reduceByKey (RDD operation - distributed)
		sum_count_rdd = ratings_rdd.reduceByKey(
			lambda a, b: (a[0] + b[0], a[1] + b[1])
		)
		
		# Bước 4: Tính trung bình bằng mapValues (RDD operation)
		avg_rdd = sum_count_rdd.mapValues(
			lambda sc: (round(sc[0] / sc[1], 2), sc[1])
		)
		
		# Bước 5: Đọc movies
		movies_rdd = sc.textFile("movies.txt") \
			.map(parse_movie) \
			.filter(lambda x: x is not None)
		
		# Bước 6: Ghép bằng leftOuterJoin (RDD operation - distributed)
		joined_rdd = avg_rdd.leftOuterJoin(movies_rdd)
		
		# Bước 7: Map thành (title, avg, count) trước khi collect
		final_rdd = joined_rdd.map(lambda kv: (
			kv[1][1] if kv[1][1] is not None else f"Movie {kv[0]}",
			kv[1][0][0],  # avg
			kv[1][0][1]   # count
		))
		
		# Bước 8: Collect về local để tránh lỗi worker
		results = final_rdd.collect()
		
		# Bước 9: Sắp xếp và xử lý trên Python
		results_sorted = sorted(results, key=lambda x: x[0])
		
		# In kết quả
		print("=== Danh sách phim và điểm đánh giá ===")
		for title, avg, total in results_sorted:
			print(f"{title} Điểm trung bình: {avg} (Tổng lượt đánh giá: {total})")
		
		# Tìm phim có điểm cao nhất (ít nhất 5 ratings)
		print("\n=== Kết quả ===")
		top_movies = sorted(
			[r for r in results if r[2] >= 5],
			key=lambda x: (-x[1], -x[2])
		)
		
		if top_movies:
			title, avg, total = top_movies[0]
			print(f"{title} là phim có điểm trung bình cao nhất: {avg} (tối thiểu 5 lượt đánh giá)")
		else:
			print("Không có phim nào có ít nhất 5 lượt đánh giá.")
	
	finally:
		spark.stop()


if __name__ == "__main__":
	main()
