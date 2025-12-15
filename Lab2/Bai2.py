from pyspark.sql import SparkSession

def parse_line(line):
    """
    Hàm parse dòng thành tuple (key, value).
    Giả sử định dạng dòng là: key,value
    Trả về: (key, value)
    """
    try:
        parts = line.strip().split(",")
        if len(parts) < 2:
            return None
        key = parts[0]
        value = parts[1]
        return (key, value)
    except Exception:
        return None
    
def parse_movie_genres(line):
    """
    Hàm parse dòng phim thành tuple (movieId, [genres]).
    Định dạng: movieId,title,genres
    Trả về: (movieId, [genre1, genre2, ...])
    """
    try:
        parts = line.strip().split(",")
        if len(parts) < 3:
            return None
        movie_id = int(parts[0])
        genres = parts[2].split("|")
        return (movie_id, genres)
    except Exception:
        return None
    
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
def main():
    """
    Hàm chính: Sử dụng PySpark RDD để xử lý dữ liệu.
    1. Đọc file ratings và movies thành RDD
    2. Parse ratings, dùng reduceByKey để tính tổng/đếm (RDD operation)
    3. Tính trung bình bằng mapValues (RDD operation)
    4. Ghép với movies để lấy genres (RDD operation)
    5. Phân tách genres và tính điểm trung bình cho từng thể loại
    6. Collect về local để tránh lỗi worker trên Windows
    7. Xử lý và in kết quả trên Python
    """
    # Khởi tạo Spark
    spark = SparkSession.builder \
        .appName("Bai2_RDD_Genres") \
        .master("local[*]") \
        .getOrCreate()
    sc = spark.sparkContext
    print("=== Bài 2: Tính điểm đánh giá theo thể loại phim (dùng RDD) ===")
    print(f"Phiên bản Spark: {spark.version}\n")


    try:
        # Bước 1: Đọc file ratings và movies thành RDD
        ratings_rdd1 = sc.textFile("ratings_1.txt")
        ratings_rdd2 = sc.textFile("ratings_2.txt")
        movies_rdd = sc.textFile("movies.txt")
        print("ratings_1.txt lines:", ratings_rdd1.count())
        print("ratings_2.txt lines:", ratings_rdd2.count())
        print("movies.txt lines:", movies_rdd.count())

        # Bước 2: Parse ratings và tính tổng/đếm
        ratings_parsed_rdd1 = ratings_rdd1.map(parse_rating).filter(lambda x: x is not None)
        ratings_parsed_rdd2 = ratings_rdd2.map(parse_rating).filter(lambda x: x is not None)
        print("Parsed ratings_1:", ratings_parsed_rdd1.count())
        print("Parsed ratings_2:", ratings_parsed_rdd2.count())
        ratings_combined_rdd = ratings_parsed_rdd1.union(ratings_parsed_rdd2)
        print("Combined ratings:", ratings_combined_rdd.count())
        ratings_aggregated_rdd = ratings_combined_rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
        print("Aggregated ratings:", ratings_aggregated_rdd.count())

        # Bước 3: Tính điểm trung bình
        ratings_avg_rdd = ratings_aggregated_rdd.mapValues(lambda x: x[0] / x[1])
        print("Average ratings:", ratings_avg_rdd.count())

        # Bước 4: Ghép với movies để lấy genres
        movies_parsed_rdd = movies_rdd.map(parse_movie_genres).filter(lambda x: x is not None)
        print("Parsed movies:", movies_parsed_rdd.count())
        ratings_with_genres_rdd = ratings_avg_rdd.join(movies_parsed_rdd)
        print("Ratings with genres:", ratings_with_genres_rdd.count())

        # Bước 5: Phân tách genres và tính điểm trung bình cho từng thể loại
        genre_ratings_rdd = ratings_with_genres_rdd.flatMap(
            lambda x: [ (genre, (x[1][0], 1)) for genre in x[1][1] ]
        )
        print("Genre ratings:", genre_ratings_rdd.count())
        genre_aggregated_rdd = genre_ratings_rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
        print("Aggregated genre ratings:", genre_aggregated_rdd.count())
        genre_avg_rdd = genre_aggregated_rdd.mapValues(lambda x: x[0] / x[1])
        print("Average genre ratings:", genre_avg_rdd.count())

        # Bước 6: Collect về local
        genre_avg_list = genre_avg_rdd.collect()
        print("Collected genre avg list:", len(genre_avg_list))

        # Bước 7: In kết quả
        for genre, avg_rating in genre_avg_list:
            print(f"Genre: {genre}, Average Rating: {avg_rating:.2f}")

    except Exception as e:
        print(f"Lỗi xảy ra: {e}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
