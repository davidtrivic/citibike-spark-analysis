from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import mean, stddev, unix_seconds, lit


spark = SparkSession.builder.appName("projekat2").getOrCreate()

def clear_df(df: DataFrame) -> DataFrame:
    df = df.filter(df.started_at < df.ended_at)
    df = df.na.drop()

    stats = df.agg(
        mean(unix_seconds(df.started_at) - unix_seconds(df.ended_at)),
        stddev(unix_seconds(df.started_at) - unix_seconds(df.ended_at))
    ).collect()[0]

    lower_bound = stats[0] - 3 * stats[1]
    upper_bound = stats[0] + 3 * stats[1]

    df = df.filter(((unix_seconds(df.started_at) - unix_seconds(df.ended_at)) >= lower_bound) & ((unix_seconds(df.started_at) - unix_seconds(df.ended_at)) <= upper_bound))

    return df

def add_cols(df: DataFrame) -> DataFrame:
    days_map = F.create_map(
        lit(1), lit("Sunday"),
        lit(2), lit("Monday"),
        lit(3), lit("Tuesday"),
        lit(4), lit("Wednesday"),
        lit(5), lit("Thursday"),
        lit(6), lit("Friday"),
        lit(7), lit("Saturday")
    )

    new_df = (
        df
        .withColumn("ride_duration", unix_seconds(df.ended_at) - unix_seconds(df.started_at))
        .withColumn("hour_of_day", F.hour(df.started_at))
        .withColumn("day_of_week", days_map[F.dayofweek(df.started_at)])
        .withColumn("is_weekend", F.when(F.dayofweek(df.started_at).isin(1, 7) , 1).otherwise(0))
        .withColumn("round_trip", F.when(df.start_station_id == df.end_station_id, 1).otherwise(0))
        .withColumn("distance", F.sqrt(F.pow(df.end_lng - df.start_lng, 2) + F.pow(df.end_lat - df.start_lat, 2)))
    )

    return new_df

def analyze_rides(df: DataFrame) -> DataFrame:
    new_df = (
        df
        .withColumn("day_type", F.when(df.is_weekend == 1, "weekend").otherwise("weekday"))
    )

    result = new_df.groupBy("day_type", "member_casual", "hour_of_day").agg(
        F.count("*").alias("num_of_rides"),
        F.round(F.avg("ride_duration"), 2).alias("avg_ride_duration")
    ).orderBy("day_type", "member_casual", "hour_of_day")

    return result

def most_active_stations(df: DataFrame, N: int, minRides: int) -> DataFrame:
    new_df = df.groupBy("start_station_id").agg(
        F.count("*").alias("num_of_rides"),
        F.round(F.sum(F.when(F.col("member_casual") == "member", 1).otherwise(0)) / F.col("num_of_rides"), 2).alias("member_ratio"),
        F.round(F.sum(F.when(F.col("round_trip") == 1, 1).otherwise(0)) / F.col("num_of_rides"), 5).alias("round_trip_ratio"),
        F.round(F.avg("ride_duration"), 2).alias("avg_ride_duration"),
        F.round(F.sum(F.when(F.col("hour_of_day").between(0, 5), 1).otherwise(0)) / F.col("num_of_rides"), 2).alias("night_rides_ratio"),
        F.round(F.sum(F.when(F.col("hour_of_day").between(6, 11), 1).otherwise(0)) / F.col("num_of_rides"), 2).alias("morning_rides_ratio"),
        F.round(F.sum(F.when(F.col("hour_of_day").between(12, 17), 1).otherwise(0)) / F.col("num_of_rides"), 2).alias("afternoon_rides_ratio"),
        F.round(F.sum(F.when(F.col("hour_of_day").between(18, 23), 1).otherwise(0)) / F.col("num_of_rides"), 2).alias("evening_rides_ratio")
    )

    new_df = new_df.filter(F.col("num_of_rides") >= minRides)
    
    result = new_df.orderBy(F.col("member_ratio").desc(), F.col("morning_rides_ratio").desc()).limit(N)

    return result

def most_common_routes(df: DataFrame, N:int, minRides: int) -> DataFrame:
    new_df = df.groupBy("start_station_id", "end_station_id").agg(
        F.count("*").alias("num_of_rides"),
        F.round(F.avg("ride_duration"), 2).alias("avg_ride_duration"),
        F.round(F.stddev("ride_duration"), 2).alias("duration_variability")
    ).filter(F.col("num_of_rides") >= minRides).orderBy(F.col("avg_ride_duration"), F.col("duration_variability")).limit(N)

    return new_df

def analyze_round_trips(df: DataFrame) -> DataFrame:
    new_df = df.groupBy("member_casual", "rideable_type").agg(
        F.count("*").alias("num_of_rides"),
        F.sum(F.when(F.col("round_trip") == 1, 1).otherwise(0)).alias("num_of_round_trips"),
        F.round(F.sum(F.when(F.col("round_trip") == 1, 1).otherwise(0)) / F.count("*"), 4).alias("round_trip_ratio"),
        F.round(F.avg(F.col("ride_duration")), 2).alias("avg_ride_duration")
    )   

    return new_df

def clear_meteostat_df(df: DataFrame) -> DataFrame:
    df = df.filter(df.month == 10)

    df = df.withColumn("date", F.to_date(F.concat_ws("-", "year", "month", "day")))

    df = (
        df
        .withColumn("prcp_indicator", F.when(F.col("prcp") > 0.0, 1).otherwise(0))
        .withColumn("temp_band", F.when(F.col("temp") < 5, "Vrlo hladno")
                    .when((F.col("temp") >= 5) & (F.col("temp") < 10), "Hladno")
                    .when((F.col("temp") >= 10) & (F.col("temp") < 15), "Umjereno")
                    .when((F.col("temp") >= 15) & (F.col("temp") < 20), "Toplo")
                    .otherwise("Vrlo toplo"))
    )

    return df

def analyze_combined_df(df: DataFrame) -> DataFrame:
    new_df = df.filter(F.col("prcp_indicator").isNotNull()).groupBy("is_weekend", "prcp_indicator").agg(
        F.count("*").alias("num_of_rides"),
        F.round(F.avg(F.col("ride_duration")), 2).alias("avg_ride_duration")
    )

    return new_df

def analyze_temp_bands(df: DataFrame) -> DataFrame:
    new_df = df.filter(F.col("temp_band").isNotNull()).groupBy("temp_band", "member_casual").agg(
        F.count("*").alias("num_of_rides"),
        F.round(F.avg(F.col("ride_duration")), 2).alias("avg_ride_duration")
    ).orderBy("temp_band", "member_casual")

    return new_df


def main():
    path = "./oposProjekat/projekat2/projekat2_data/citibike"

    df = spark.read.format("csv")\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .load(path)
    df.limit(50).write.mode("overwrite").option("header", True).csv("./oposProjekat/projekat2/1-df")

    df = clear_df(df)
    df.limit(50).write.mode("overwrite").option("header", True).csv("./oposProjekat/projekat2/2-clear_df")

    df = add_cols(df)
    df.limit(50).write.mode("overwrite").option("header", True).csv("./oposProjekat/projekat2/3-extended_df") 
    
    analyzed_result = analyze_rides(df)
    analyzed_result.write.mode("overwrite").option("header", True).csv("./oposProjekat/projekat2/4-analyze_rides")

    active_stations_result = most_active_stations(df, 50, 10000)
    active_stations_result.write.mode("overwrite").option("header", True).csv("./oposProjekat/projekat2/5-most_active_stations")
    
    most_common_routes_result = most_common_routes(df, 50, 100)
    most_common_routes_result.write.mode("overwrite").option("header", True).csv("./oposProjekat/projekat2/6-most_common_routes")

    analyzed_round_trips_result = analyze_round_trips(df)
    analyzed_round_trips_result.write.mode("overwrite").option("header", True).csv("./oposProjekat/projekat2/7-analyze_round_trips")
        
    df = df.withColumn("started_at_utc", F.to_utc_timestamp(F.col("started_at"), "America/New_York"))
    df = df.withColumn("utc_date", F.to_date("started_at_utc"))
    df = df.withColumn("utc_hour", F.hour("started_at_utc"))

    meteostat_df = spark.read.format("csv")\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .load("./oposProjekat/projekat2/projekat2_data/KJRB0.csv.gz")
    
    meteostat_df = clear_meteostat_df(meteostat_df)
    meteostat_df = meteostat_df.withColumnRenamed("hour", "utc_hour").withColumnRenamed("date", "utc_date")
    meteostat_df.limit(50).write.mode("overwrite").option("header", True).csv("./oposProjekat/projekat2/8-load_meteostat_df")
    
    combined_df = df.join(meteostat_df, ["utc_date", "utc_hour"], "left")
    combined_df.limit(50).write.mode("overwrite").option("header", True).csv("./oposProjekat/projekat2/9-combined_df")

    analyzed_combined_df = analyze_combined_df(combined_df)
    analyzed_combined_df.write.mode("overwrite").option("header", True).csv("./oposProjekat/projekat2/10-analyze_combined_df")

    analyzed_temp_bands = analyze_temp_bands(combined_df)
    analyzed_temp_bands.write.mode("overwrite").option("header", True).csv("./oposProjekat/projekat2/11-analyze_temp_bands")

    spark.stop()

if __name__ == "__main__":
    main()