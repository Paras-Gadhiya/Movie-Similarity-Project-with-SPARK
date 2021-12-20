from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import sys


def computeSimilarity(spark, data):
    # Adding columns and Compute xx, xy and yy columns (x = rating1 and movie1 :::  y = rating2 and movie2)
    pairScores = data \
        .withColumn("xx", func.col("rating1") * func.col("rating1")) \
        .withColumn("yy", func.col("rating2") * func.col("rating2")) \
        .withColumn("xy", func.col("rating1") * func.col("rating2"))

    # Compute numerator, denominator and numPairs columns
    calculateSimilarity = pairScores \
        .groupBy("movie1", "movie2") \
        .agg( \
        func.sum(func.col("xy")).alias("numerator"), \
        (func.sqrt(func.sum(func.col("xx"))) * func.sqrt(func.sum(func.col("yy")))).alias("denominator"), \
        func.count(func.col("xy")).alias("numUsers")
    )

    # Calculate score and select only needed columns (movie1, movie2, score, numUsers)
    result = calculateSimilarity \
        .withColumn("score", \
                    func.when(func.col("denominator") != 0, func.col("numerator") / func.col("denominator")) \
                    .otherwise(0) \
                    ).select("movie1", "movie2", "score", "numUsers")

    return result


# Get movie name by given movie id
def getMovieName(movieNames, movieId):
    result = movieNames.filter(func.col("movieID") == movieId) \
        .select("movieTitle").collect()[0]

    return result[0]


# ===========================================================================================
# ----------------------------Code starts from here------------------------------------------
# ===========================================================================================

# use every CPU cores on my local system
spark = SparkSession.builder.appName("MovieSimilarities").master("local[*]").getOrCreate()

movieNamesSchema = StructType([ \
    StructField("movieID", IntegerType(), True), \
    StructField("movieTitle", StringType(), True) \
    ])

moviesSchema = StructType([ \
    StructField("userID", IntegerType(), True), \
    StructField("movieID", IntegerType(), True), \
    StructField("rating", IntegerType(), True), \
    StructField("timestamp", LongType(), True)])

# Create a broadcast dataset of movieID and movieTitle.
# here we use '\' (escape character because of this script looks better and we gonna this script run by spark-engine)

movieNames = spark.read \
    .option("sep", "::") \
    .option("charset", "ISO-8859-1") \
    .schema(movieNamesSchema) \
    .csv("/media/paras/Study/Projects/Movie-Similarity-Project-with-SPARK/data/ml-10M100K/movies.dat")
# Load up movie data as dataset

movies = spark.read \
    .option("sep", "::") \
    .schema(moviesSchema) \
    .csv("/media/paras/Study/Projects/Movie-Similarity-Project-with-SPARK/data/ml-10M100K/ratings.dat")

ratings = movies.select("userId", "movieId", "rating")

# Emit every movie rated together by the same user.
# Self-join to find every combination.
# Select movie pairs and rating pairs
moviePairs = ratings.alias("ratings1") \
    .join(ratings.alias("ratings2"), (func.col("ratings1.userId") == func.col("ratings2.userId")) \
          & (func.col("ratings1.movieId") < func.col("ratings2.movieId"))) \
    .select(func.col("ratings1.movieId").alias("movie1"), \
            func.col("ratings2.movieId").alias("movie2"), \
            func.col("ratings1.rating").alias("rating1"), \
            func.col("ratings2.rating").alias("rating2"))

moviePairSimilarities = computeSimilarity(spark,
                                          moviePairs).cache()  # here, we use cache() function for improve performance

if (len(sys.argv) > 1):  # validating id from user via COMMAND ARGUMENT
    # setting some minimum boundary for occurrence of pairs
    scoreMinLimit = 0.97
    coOccurrenceMinLimit = 50.0

    movieID = int(sys.argv[1])  # getting id from user via COMMAND ARGUMENT

    # Filter for movies which cross our Minimum quality limits...
    filteredResults = moviePairSimilarities.filter( \
        ((func.col("movie1") == movieID) | (func.col("movie2") == movieID)) & \
        (func.col("score") > scoreMinLimit) & (func.col("numUsers") > coOccurrenceMinLimit))

    # Sort by quality score.
    results = filteredResults.sort(func.col("score").desc()).take(10)

    print("Top 10 similar movies for " + getMovieName(movieNames, movieID))

    for result in results:
        # Display the similarity result for that movie but not itself.
        similarMovieID = result.movie1
        if similarMovieID == movieID:
            similarMovieID = result.movie2

        print(getMovieName(movieNames, similarMovieID) + "\tscore: " \
              + str(result.score) + "\tUsers_Watched: " + str(result.numUsers))
