from pyspark.sql import SparkSession

def parse_line(line):
    """Parse line to list"""
    return line.strip().split(",")

def main():
    spark = SparkSession.builder\
        .appName("Bai5_RDD_Ratings_Occupation")\
        .master("local[*]")\
        .getOrCreate()
    
    sc = spark.sparkContext
    print("=== Ex5: Average Movie Ratings by Occupation (using RDD) ===")
    print(f"Spark version: {spark.version}\n")

    try:
        # Step 1: Load data
        ratings_rdd = sc.textFile("ratings_1.txt")\
            .union(sc.textFile("ratings_2.txt"))\
            .map(parse_line)  # (userId, movieId, rating, timestamp)
        
        users_rdd = sc.textFile("users.txt")\
            .map(parse_line)\
            .map(lambda x: (x[0], int(x[3])))  # (userId, occupationId) - format: userId,gender,age,occupation,zipCode
        
        occupations_rdd = sc.textFile("occupation.txt")\
            .map(parse_line)\
            .map(lambda x: (int(x[0]), x[1]))  # (occupationId, occupationName)
        
        # Step 2: Map ratings to (userId, rating)
        ratings_mapped = ratings_rdd.map(lambda x: (x[0], float(x[2])))
        
        # Step 3: Join ratings with users to get occupationId
        user_ratings = ratings_mapped.join(users_rdd)
        # Format: (userId, (rating, occupationId))
        
        # Step 4: Map to (occupationId, (sum, count))
        occupation_stats = user_ratings.map(lambda x: (x[1][1], (x[1][0], 1)))\
            .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
        
        # Step 5: Join with occupation names
        final_rdd = occupation_stats.join(occupations_rdd)
        # Format: (occupationId, ((sum, count), occupationName))
        
        # Step 6: Collect and format results
        results = final_rdd.collect()
        
        for occ_id, (stats, occ_name) in sorted(results, key=lambda x: x[1][1]):
            total_sum, total_count = stats
            avg_rating = round(total_sum / total_count, 2)
            print(f"{occ_name} - AverageRating: {avg_rating} (TotalRatings: {total_count})")
    
    except Exception as e:
        print(f"Error occurred: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
