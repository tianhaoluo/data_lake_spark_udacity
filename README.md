# Introduction

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.


# How to run
1. SSH into your EMR cluster, create an S3 bucket for later use of dumping output files (for example, an S3 bucket with name 'udacity-yourname-cfnsadcnd')

2. Go to the configuration file 'dl.cfg' to update your AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, INPUT, OUTPUT. It is recommended that you do not put any quotes around the strings. For example
            `INPUT=s3a://udacity-dend/` 
        NOT 
            `INPUT="s3a://udacity-dend/"`.
   OUTPUT can be the S3 bucket you just created, e.g. `OUTPUT=s3a://udacity-yourname-cfnsadcnd/`.
   
3. Locate where *spark-submit* is with the command
              `which spark-submit`
   On an EMR cluster, it usually should be `/usr/bin/spark-submit`
   
4. Open the terminal, run 
            `/usr/bin/spark-submit etl.py`

5. Enjoy your new Data Lake at Sparkify! For example, you can load the songplays_table by `songplays_table = spark.read.parquet(os.path.join(output_data,"songplays"))`. Then you can open a Jupyter Notebook, and make use of *spark.sql* to run some sample queries.

# Schemas
songs_table (exactly the same with the requirements)
```
root
 |-- song_id: string (nullable = true)
 |-- title: string (nullable = true)
 |-- duration: double (nullable = true)
 |-- year: integer (nullable = true)
 |-- artist_id: string (nullable = true)
```

artists_table (same with requirements)
```
root
 |-- artist_id: string (nullable = true)
 |-- name: string (nullable = true)
 |-- location: string (nullable = true)
 |-- latitude: double (nullable = true)
 |-- longitude: double (nullable = true)
```

time_table (same with requirements)
```
root
 |-- start_time: integer (nullable = true)
 |-- hour: integer (nullable = true)
 |-- day: integer (nullable = true)
 |-- week: integer (nullable = true)
 |-- weekday: integer (nullable = true)
 |-- year: integer (nullable = true)
 |-- month: integer (nullable = true)
```

users_table (same with requirements)
```
root
 |-- user_id: string (nullable = true)
 |-- first_name: string (nullable = true)
 |-- last_name: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- level: string (nullable = true)
```

songplays_table (extra columns 'year' and 'month' for writing parquet files partitioned by year and month)
```
root
 |-- songplay_id: long (nullable = true)
 |-- start_time: integer (nullable = true)
 |-- user_id: string (nullable = true)
 |-- level: string (nullable = true)
 |-- song_id: string (nullable = true)
 |-- artist_id: string (nullable = true)
 |-- session_id: long (nullable = true)
 |-- location: string (nullable = true)
 |-- user_agent: string (nullable = true)
 |-- year: integer (nullable = true)
 |-- month: integer (nullable = true)
```