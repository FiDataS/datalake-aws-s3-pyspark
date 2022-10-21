import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, monotonically_increasing_id
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table (song_id, title, artist_id, year, duration)
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').mode('overwrite').parquet(output_data + 'songs')

    # extract columns to create artists table(artist_id, name, location, lattitude, longitude)
    artists_table = df.select('artist_id', col('artist_name').alias('name'), col('artist_location').alias('location'), col('artist_latitude').alias('latitude'), col('artist_longitude').alias('longitude')).distinct()
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + 'artists')


def process_log_data(spark, input_data, output_data):
    # read log data file
    df_log = spark.read.json(input_data + 'log_data/*.json')
    
    # filter by actions for song plays
    df_log = df_log.filter(df_log.page == 'NextSong')

    # extract columns for users table    
    users_table = df_log.select(col('userId').alias('user_id'), col('firstName').alias('first_name'), col('lastName').alias('last_name'), 'gender', 'level').distinct()
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + 'users')

    # create timestamp column from original timestamp column in datetime format
    # neccessary to define the return type of udf as timestamp type
    get_timestamp = udf(lambda ts: datetime.fromtimestamp(ts/1000),TimestampType())
    df_log = df_log.withColumn('start_time', get_timestamp(df_log.ts))
    
    # extract columns to create time table
    time_table = df_log.select('start_time') \
                    .withColumn('hour', hour('start_time')) \
                    .withColumn('day', dayofmonth('start_time')) \
                    .withColumn('week', weekofyear('start_time')) \
                    .withColumn('month', month('start_time'))\
                    .withColumn('year', year('start_time')) \
                    .withColumn('weekday', dayofweek('start_time'))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').mode('overwrite').parquet(output_data + 'time')

    # read in song data to use for songplays table
    df_song = spark.read.json(input_data + 'song_data/*/*/*/*.json')

    # extract columns from joined song and log datasets to create songplays table 
    
    songplays_table = df_log.join(df_song, (df_log.song == df_song.title)\
                                        & (df_log.artist == df_song.artist_name)\
                                        & (df_log.length == df_song.duration), "inner") \
                            .distinct() \
                            .select('start_time', col('userId').alias('user_id'), 'level', 'song_id', \
                                    'artist_id', col('sessionId').alias('session_id'),'location', col('userAgent').alias('user_agent')) \
                            .withColumn("songplay_id", monotonically_increasing_id()).withColumn('year', year('start_time')).withColumn('month', month('start_time'))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').mode('overwrite').parquet(output_data + 'songplays')


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-table-bucket/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
