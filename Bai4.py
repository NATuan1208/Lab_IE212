from pyspark.sql import SparkSession

def parse_line(line):
    """Parse line to list"""
    return line.strip().split(",")

def get_age_group(age):
    """Categorize age into groups: 0-18, 18-35, 35-50, 50+"""
    age_int = int(age)
    if age_int < 18:
        return "0-18"
    elif age_int < 35:
        return "18-35"
    elif age_int < 50:
        return "35-50"
    else:
        return "50+"

def main():
    spark = SparkSession.builder\
        .appName("Bai4_RDD_Ratings_AgeGroup")\
        .master("local[*]")\
        .getOrCreate()
    
    sc = spark.sparkContext
    print("=== Ex4: Average Movie Ratings by Age Group (using RDD) ===")
    print(f"Spark version: {spark.version}\n")

    try:
        # Step 1: Load data
        movies_rdd = sc.textFile("movies.txt")\
            .map(parse_line)\
            .map(lambda x: (x[0], x[1]))  # (movieId, title)
        
        ratings_rdd = sc.textFile("ratings_1.txt")\
            .union(sc.textFile("ratings_2.txt"))\
            .map(parse_line)  # (userId, movieId, rating, timestamp)
        
        users_rdd = sc.textFile("users.txt")\
            .map(parse_line)\
            .map(lambda x: (x[0], int(x[2])))  # (userId, age) - format: userId,gender,age,occupation,zipCode
        
        # Step 2: Map ratings to (userId, (movieId, rating))
        ratings_mapped = ratings_rdd.map(lambda x: (x[0], (x[1], float(x[2]))))
        
        # Step 3: Join ratings with users to get age
        user_ratings = ratings_mapped.join(users_rdd)
        # Format: (userId, ((movieId, rating), age))
        
        # Step 4: Map to (movieId, {ageGroup: (sum, count)})
        def map_age_group_stats(data):
            movie_id = data[1][0][0]
            rating = data[1][0][1]
            age = data[1][1]
            age_group = get_age_group(age)
            
            # Return (movieId, {ageGroup: (sum, count)})
            return (movie_id, {age_group: (rating, 1)})
        
        age_group_stats = user_ratings.map(map_age_group_stats)\
            .reduceByKey(lambda a, b: {
                k: (a.get(k, (0, 0))[0] + b.get(k, (0, 0))[0], 
                    a.get(k, (0, 0))[1] + b.get(k, (0, 0))[1])
                for k in set(list(a.keys()) + list(b.keys()))
            })
        
        # Step 5: Join with movie titles
        final_rdd = age_group_stats.join(movies_rdd)
        # Format: (movieId, ({ageGroup: (sum, count)}, title))
        
        # Step 6: Collect and format results
        results = final_rdd.collect()
        
        # Age groups order
        age_groups_order = ["0-18", "18-35", "35-50", "50+"]
        
        for mid, (stats, title) in sorted(results, key=lambda x: x[1][1]):
            age_avgs = []
            for age_group in age_groups_order:
                if age_group in stats:
                    sum_rating, count = stats[age_group]
                    avg = round(sum_rating / count, 2)
                    age_avgs.append(f"{avg}")
                else:
                    age_avgs.append("NA")
            
            avg_str = ", ".join([f"{age_groups_order[i]}: {age_avgs[i]}" for i in range(len(age_groups_order))])
            print(f"{title} - [{avg_str}]")
    
    except Exception as e:
        print(f"Error occurred: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
