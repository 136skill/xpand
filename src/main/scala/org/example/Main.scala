package org.example

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{array_size, avg, col, collect_list, collect_set, count, date_add, explode, explode_outer, expr, lit, regexp_replace, row_number, size, substring, to_date, to_timestamp, when}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}


object Main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Challenge")
      .master("local[*]")
      .config("spark.driver.bindAddress","127.0.0.1")
      .getOrCreate()


    val df_user = spark.read
      .option("header",value = true)
      .csv("data/googleplaystore_user_reviews.csv")


    val df = spark.read
      .option("header",value = true)
      .csv("data/googleplaystore.csv")


    import spark.implicits._

    //EXERCÍCIO
    //PART 1

    val columnInteger = df_user("Sentiment_Polarity").cast(IntegerType)
    val df_1 = df_user.select(col("App"), columnInteger).na.fill(0, Seq("Sentiment_Polarity")).groupBy($"App").agg(functions.avg($"Sentiment_Polarity").as("Average_Sentiment_Polarity"))

    //PART 2

    val df_2 = df.filter(df("Rating")>=4).sort($"Rating".desc)
    df_2.repartition(1).write.option("header", "true").option("delimiter", "§").csv("data/best_apps.csv")

    //Part 3
    //Conversão de tipos:
    val valid_types = df.withColumn("Last Updated", to_timestamp(col("Last Updated"), "MMMM d, yyyy"))
      .withColumn("Rating", df("Rating").cast(DoubleType))
      .withColumn("Price",df("Price") * 0.9)
      .withColumn("Reviews", df("Reviews").cast(LongType))
      .withColumn("Genres", functions.split(col("Genres"), ";"))
      .withColumn("Size", when(col("Size").endsWith("M"), regexp_replace(col("Size"), "M$", "").cast(DoubleType)).otherwise(col("Size").cast(DoubleType)))


    //Conversão de nomes:
    val rename = List(col ("App"), col("Categories"),
      col ("Rating"), col ("Reviews"), col ("Size"),
      col ("Installs"), col ("Type"), col ("Price"),
      col("Content Rating").as("Content_Rating"),
      col ("Genres"),
      col("Last Updated").as("Last_Updated"),
      col("Current Ver").as("Current_Version"),
      col("Android Ver").as("Minimum_Android_Version"))

    //Agrupar:

    val grouped = valid_types.groupBy($"App").agg(collect_set($"Category").as("Categories"))

    val windowRating = Window.partitionBy($"App").orderBy(col("Reviews").desc)
    val moreRatings = valid_types.withColumn("top_rating", row_number().over(windowRating))
      .filter(col("top_rating") === 1)

    val joined = moreRatings.join(grouped,"App").drop("top_rating","Category")
    val df_3 = joined.select(rename: _*)

    //PART 4

    val part_4 = df_3.join(df_1,"App")

    part_4.write
      .option("compression", "gzip")
      .parquet("data/googleplaystore_cleaned")

    //PART 5

    val tudo = df_3.join(df_1,"App")

    //separar o array dos géneros - explode
    val splited = tudo.withColumn("Genre", explode(col("Genres"))).na.fill(0, Seq("Rating"))
    splited.show(3000)

    val df_genre = splited.groupBy($"Genre").agg(array_size(collect_list("App")).alias("Count"), avg("Rating").as("Average_Rating"), avg("Average_Sentiment_Polarity").as("Average_Sentiment_Polarity")).orderBy("Genre")

    val df_4 = df_genre.write
      .option("compression", "gzip")
      .parquet("data/googleplaystore_metrics")

    //splited.createOrReplaceTempView("apps_genres_ratings")
    //df_user.createOrReplaceTempView("apps_reviews")

    //val result = spark.sql("""
    //SELECT
    //    ag.Genre,
    //    COUNT(DISTINCT ar.App) AS Count,
    //    AVG(ag.Rating) AS Average_Rating,
    //    AVG(ar.Sentiment_Polarity) AS Average_Sentiment_Polarity
    //FROM
    //    apps_genres_ratings ag
    //JOIN
    //    apps_reviews ar ON ag.App = ar.App
    //GROUP BY
    //    ag.Genre
    //ORDER BY
    //    ag.Genre ASC
    //""")

    //result.show(40)
  }
}