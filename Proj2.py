import pyspark
import zipfile
import urllib.request
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark.sql.types import StructType,StructField, StringType, IntegerType


if __name__ == '__main__':
    #Creating spark session
    spark = SparkSession.builder.master("spark://localhost:7077").appName("MOVIE_RATING_PROJECT").getOrCreate()

movies_schema = StructType([
    StructField("MovieID", StringType(), True),
    StructField("Title", StringType(), True),
    StructField("Genres", StringType(), True)
])


user_schema = StructType([
    StructField("UserID", IntegerType(), True),
    StructField("Gender", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Occupation", IntegerType(), True),
    StructField("Zip-code", IntegerType(), True)
])


rating_schema = StructType([
    StructField("UserID", IntegerType(), True),
    StructField("MovieID", IntegerType(), True),
    StructField("Rating", IntegerType(), True),
    StructField("Timestamp", IntegerType(), True)
])


df1 = spark.read.option("header",False).option("inferSchema",True).schema(movies_schema).csv("/input_data/movies.dat", sep="::")
df2=spark.read.option("header",False).option("inferSchema",True).schema(user_schema).csv("/input_data/users.dat", sep="::")
df3=spark.read.option("header",False).option("inferSchema",True).schema(rating_schema).csv("/input_data/ratings.dat", sep="::")
#Q1
df3.groupby("MovieID").agg(count("*").alias("Total_count")).orderBy(col("Total_count").desc()).limit(10).select("MovieID").show()

#Q2
df1.withColumn("Genres1", split(df1["Genres"], "\\|").getItem(0)).select("Genres1").distinct().show()

#Q3
df1.withColumn("genre", explode(split("genres", "\\|"))).groupby("genre").count().show()

#Q4
resultt_count=df1.filter(col("Title").rlike('^[a-zA-Z0-9]')).count()
print(f"The number movies are starting with numbers or letters (Example: Starting with 1/2/3../A/B/C..Z) : {resultt_count}")

#Q5
df_result=df1.withColumn("Year",regexp_extract(df1["Title"], "\((\d{4})\)",1)).withColumn("Movie_name",regexp_extract(df1["Title"], "([^\(]+)",1)).orderBy(desc("Year")).select("Movie_name","Year")

latest_movies_list=df_result.filter(col("Year")==df_result.agg(max(col("Year"))).collect()[0][0])
latest_movies_list.show()

#SPARK SQL
#Q1
df1.createOrReplaceTempView("movies")
df1.show()
df2.createOrReplaceTempView("users")
df2.show()
df3.createOrReplaceTempView("ratings")
df3.show()

#Q2

sql_query = spark.sql("SELECT *, SUBSTRING(title, POSITION('(' IN title) + 1, 4) AS movie_year,REGEXP_EXTRACT(title, '^[^(]+', 0) AS movie_name  FROM movies where ORDER BY movie_year ")
sql_query.show()
#Q3
sql_query = spark.sql("SELECT count(*) as Movie_count ,Movie_year FROM (SELECT *, SUBSTRING(title, POSITION('(' IN title) + 1, 4) AS movie_year,REGEXP_EXTRACT(title, '^[^(]+', 0) AS movie_name  FROM movies)A GROUP BY movie_year")
sql_query.show()
#Q4
sql_query=spark.sql("SELECT COUNT(*) As Total_count ,RATING FROM RATINGS GROUP BY RATING ORDER BY RATING")
sql_query.show()
#Q5
sql_query=spark.sql("SELECT COUNT(*)As Total_count ,MovieID FROM RATINGS GROUP BY MovieID ORDER BY MovieID")
sql_query.show()
#Q6
sql_query=spark.sql("SELECT SUM(RATING)As total_rating ,MovieID FROM RATINGS GROUP BY MovieID ORDER BY MovieID")
sql_query.show()

#Q7
sql_query=spark.sql("SELECT ROUND(AVG(RATING),3)As total_rating ,MovieID FROM RATINGS GROUP BY MovieID ORDER BY MovieID")
sql_query.show()

#Spark_Dataframe
#Q1
df_result=df1.withColumn("Year",regexp_extract(df1["Title"], "\((\d{4})\)",1)).withColumn("Movie_name",regexp_extract(df1["Title"], "([^\(]+)",1)).orderBy(desc("Year")).select("Movie_name","Year")
df_result.show()

#Q2
df2=spark.read.option("header",False).option("inferSchema",True).schema(user_schema).csv("/input_data/users.dat", sep="::")


#Q3
df3=spark.read.option("header",False).option("inferSchema",True).schema(rating_schema).csv("/input_data/ratings.dat", sep="::")

#Q4
url="https://files.grouplens.org/datasets/movielens/ml-1m.zip"
destinatin_path="/config/workspace/SPARK_PROJECT/.zip"
extracted_path="/config/workspace/SPARK_PROJECT/"
urllib.request.urlretrieve( url,destinatin_path)
with zipfile.ZipFile(destinatin_path,'r').extractall(extracted_path):pass

