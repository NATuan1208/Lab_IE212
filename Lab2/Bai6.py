from pyspark.sql import SparkSession
from datetime import datetime

def parse_line(line):
    """Parse line to list"""
    return line.strip().split(",")

def get_year_from_timestamp(timestamp):
    """Convert Unix timestamp to year"""
    try:
        year = datetime.fromtimestamp(int(timestamp)).year
        return year
    except Exception:
        return None

def main():
    spark = SparkSession.builder\
        .appName("Bai6_RDD_Ratings_Year")\
        .master("local[*]")\
        .getOrCreate()
    
    sc = spark.sparkContext
    print("=== Ex6: Average Movie Ratings by Year (using RDD) ===")
    print(f"Spark version: {spark.version}\n")

    try:
        # Step 1: Load ratings data
        # Format: userId, movieId, rating, timestamp
        ratings_rdd = sc.textFile("ratings_1.txt")\
            .union(sc.textFile("ratings_2.txt"))\
            .map(parse_line)
        
        # Step 2: Map to (year, (rating, 1))
        def map_year_stats(line):
            user_id = line[0]
            movie_id = line[1]
            rating = float(line[2])
            timestamp = line[3]
            
            year = get_year_from_timestamp(timestamp)
            if year is None:
                return None
            
            return (year, (rating, 1))
        
        year_stats = ratings_rdd.map(map_year_stats).filter(lambda x: x is not None)
        
        # Step 3: Aggregate by year using reduceByKey
        year_aggregated = year_stats.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
        
        # Step 4: Calculate average ratings
        year_avg = year_aggregated.mapValues(lambda x: round(x[0] / x[1], 2))
        
        # Step 5: Collect and format results
        results = year_avg.collect()
        
        # Sort by year
        for year, (sum_rating, count) in sorted(year_aggregated.collect()):
            avg_rating = round(sum_rating / count, 2)
            print(f"{year} - TotalRatings: {count}, AverageRating: {avg_rating}")
    
    except Exception as e:
        print(f"Error occurred: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
