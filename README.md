# Introduction

# How to run
1.Go to the configuration file 'dl.cfg' to update your AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, INPUT, OUTPUT (make sure OUTPUT is in an S3 bucket you have write permission to). It is recommended that you do not put any quotes around the strings. For example
            `INPUT=s3a://udacity-dend/` 
        NOT 
            `INPUT="s3a://udacity-dend/"`        

2. Open the terminal, run 
            `python etl.py`

3. Enjoy your new Data Lake at Sparkify! For example, you can load the songplays_table by
        `songplays_table = spark.read.parquet(os.path.join(output_data,"songplays"))`