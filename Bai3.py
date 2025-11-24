from pyspark.sql import SparkSession

def parse_line(line):
    """Parse line to list"""
    return line.strip().split(",")

def main():
    spark = SparkSession.builder\
        .appName("Bai3_RDD_Ratings_Gender")\
        .master("local[*]")\
        .getOrCreate()
    
    sc = spark.sparkContext
    print("=== Ex3: Average Movie Ratings by Gender (using RDD) ===")
    print(f"Spark version: {spark.version}\n")

    try:
        
        movies_rdd = sc.textFile("movies.txt")\
            .map(parse_line)\
            .map(lambda x: (x[0], x[1]))  # (movieId, title)
        
        ratings_rdd = sc.textFile("ratings_1.txt")\
            .union(sc.textFile("ratings_2.txt"))\
            .map(parse_line)  # (userId, movieId, rating, timestamp)
        
        users_rdd = sc.textFile("users.txt")\
            .map(parse_line)\
            .map(lambda x: (x[0], x[2]))  # (userId, gender)
        
        
        ratings_mapped = ratings_rdd.map(lambda x: (x[0], (x[1], float(x[2]))))
        
        
        user_ratings = ratings_mapped.join(users_rdd)
        # Format: (userId, ((movieId, rating), gender))
        
       
        def map_gender_stats(data):
            movie_id = data[1][0][0]
            rating = data[1][0][1]
            gender = data[1][1]
            
            if gender == 'M':
                return (movie_id, (rating, 1, 0.0, 0))  # (SumM, CntM, SumF, CntF)
            else:
                return (movie_id, (0.0, 0, rating, 1))
        
        gender_stats = user_ratings.map(map_gender_stats)\
            .reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1], a[2]+b[2], a[3]+b[3]))
        
        
        final_rdd = gender_stats.join(movies_rdd)
        # (movieId, ((sum_m, cnt_m, sum_f, cnt_f), title))
        
       
        results = final_rdd.collect()
        
        for mid, (stats, title) in sorted(results, key=lambda x: x[1][1]):
            sum_m, cnt_m, sum_f, cnt_f = stats
            male_avg = round(sum_m / cnt_m, 2) if cnt_m > 0 else "NA"
            female_avg = round(sum_f / cnt_f, 2) if cnt_f > 0 else "NA"
            
            fmt_m = f"{male_avg:.2f}" if isinstance(male_avg, float) else male_avg
            fmt_f = f"{female_avg:.2f}" if isinstance(female_avg, float) else female_avg
            
            print(f"{title} - Male_Avg: {fmt_m}, Female_Avg: {fmt_f}")
    
    except Exception as e:
        print(f"Error occurred: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()