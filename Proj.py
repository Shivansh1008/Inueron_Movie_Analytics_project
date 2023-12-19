import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import *

if __name__ == '__main__':
    #Creating spark session
    spark = SparkSession.builder.master("spark://localhost:7077").appName("MOVIE_RATING_PROJECT").getOrCreate()

df1 = spark.read.option("header",True).option("inferSchema",True).csv("/input_data/movies.dat", sep="::")
df2 = spark.read.option("header",True).option("inferSchema",True).csv("/input_data/users.dat", sep="::")
df3 = spark.read.option("header",True).option("inferSchema",True).csv("/input_data/ratings.dat", sep="::")

#Q1
dF3.groupby("MovieID").agg(count("*").alias("Total_count")).orderBy(col("Total_count").desc()).limit(10).select("MovieID").show()

#Q2
df1.withColumn("Genres1", split(df1["Genres"], "\\|").getItem(0)).select("Genres1").distinct().show()

#Q3
df1.withColumn("genre", explode(split("genres", "\\|"))).groupby("genre").count().show()

#Q4
df1.filter(col("Title").rlike('^[a-zA-Z0-9]')).count()

#Q5
df_result=df1.withColumn("Year",regexp_extract(df1["Title"], "\((\d{4})\)",1)).withColumn("Movie_name",regexp_extract(df1["Title"], "([^\(]+)",1)).orderBy(desc("Year")).select("Movie_name","Year")

latest_movies_list=df_result.filter(col("Year")==df_result.agg(max(col("Year"))).collect()[0][0])

#SPARK SQL
#Q1
df1.createOrReplaceTempView("movies")
df2.createOrReplaceTempView("users")
df3.createOrReplaceTempView("ratings")

#Q2
SQL=f"select * from (SELECT *, REGEXP_EXTRACT(title, '\\(([0-9]+)\\)') AS movie_year, REGEXP_EXTRACT(title, '^[a-zA-Z]+',0) AS movie_name FROM movies)A 
where movie_year = select min(movie_year) from movies group by movie_year"
movies_year = spark.sql(SQL)

