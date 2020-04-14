import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType, IntegerType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """Create a spark session
    
        Args:
            - None
        
        Return:
            - An instance of SparkSession
       
    
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Read the song_data from raw JSON files, produce relevant tables (artists_table, songs_table, song_df) and write them back into S3
    
        Args:
            - spark: current instance of a SparkSession, created by 'create_spark_session' function
            - input_data: A string, the path to the input data on S3
            - output_data: A string, the path to the output data on S3

        Return:
            - Nothing
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    
    # read song data file
    df = spark.read.format("json").load(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year','artist_id').parquet(os.path.join(output_data,"songs"))

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude')\
                    .withColumnRenamed('artist_name','name').withColumnRenamed('artist_location','location')\
                    .withColumnRenamed('artist_latitude','latitude')\
                    .withColumnRenamed('artist_longitude','longitude')
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(os.path.join(output_data,"artists"))
    
    #Extract song_df for future use
    song_df = df.select('artist_id','artist_name','song_id','title')
    song_df.write.mode('overwrite').parquet(os.path.join(output_data,"song_df"))


def process_log_data(spark, input_data, output_data):
    """Read the log_data from raw JSON files, produce relevant tables (time_table, users_table, songplays_df) and write them back into S3
    
        Args:
            - spark: current instance of a SparkSession, created by 'create_spark_session' function
            - input_data: A string, the path to the input data on S3
            - output_data: A string, the path to the output data on S3

        Return:
            - Nothing
    """
    # get filepath to log data file
    log_data = os.path.join(input_data,"log_data/*/*/*.json")

    # read log data file
    df = spark.read.format("json").load(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table
    users_table = df.select('userId','firstName','lastName','gender','level')\
                    .withColumnRenamed('userId','user_id')\
                    .withColumnRenamed('firstName','first_name')\
                    .withColumnRenamed('lastName','last_name')
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(os.path.join(output_data,"users"))

    # create timestamp column from original timestamp column: get the seconds by rounding
    get_timestamp = udf(lambda ts: round(ts / 1000), IntegerType() )
    df = df.withColumn('timestamp',get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    get_datetime = get_datetime = udf(lambda ts: datetime.utcfromtimestamp(ts), TimestampType())
    df = df.withColumn('datetime',get_datetime('timestamp'))
    
    #A UDF to extract weekday from datetime, 0 = Monday, 1 = Tuesday, ..., 6 = Sunday
    weekday = udf(lambda dt: dt.weekday(), IntegerType())
    
    # extract columns to create time table
    time_table = df.select('timestamp','datetime').distinct().withColumnRenamed('timestamp','start_time')\
                                                    .withColumn('hour',hour('datetime'))\
                                                    .withColumn('day',dayofmonth('datetime'))\
                                                    .withColumn('week',weekofyear('datetime'))\
                                                    .withColumn('month',month('datetime'))\
                                                    .withColumn('year',year('datetime'))\
                                                    .withColumn('weekday',weekday('datetime'))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year','month').parquet(os.path.join(output_data,"time"))

    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data,"song_df"))

    # extract columns from joined song and log datasets to create songplays table 
    #columns: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    songplays_table = df.select('timestamp', 'userId', 'level', 'song', 'artist', 'sessionId', 'location','userAgent').distinct().withColumnRenamed('userId','user_id')\
                                                                                                                .withColumnRenamed('sessionId','session_id')\
                                                                                                                .withColumnRenamed('userAgent','user_agent')\
                                                                                                                .withColumnRenamed('timestamp','start_time')\
                                                                                                                .join(song_df, (df.artist == song_df.artist_name) & (df.song == song_df.title))\
                                                                                                                .withColumn("songplay_id", monotonically_increasing_id())\
                                                                                                                .withColumn('datetime',get_datetime('start_time'))\
                                                                                                                .withColumn('month',month('datetime'))\
                                                                                                                .withColumn('year',year('datetime'))\
                                                                                                                .select('songplay_id','start_time','user_id','level','song_id','artist_id',\
                                                                                                                'session_id','location','user_agent','year','month')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year','month').parquet(os.path.join(output_data,"songplays"))


def main():
    spark = create_spark_session()
    #input_data = "s3a://udacity-dend/"
    input_data = config.get('S3','INPUT')
    output_data = config.get('S3','OUTPUT')
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
