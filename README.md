## DATA LAKE PROJECT

### 1. Discuss the purpose of this database in context of the startup, Sparkify, and their analytical goals.  
###### The objective of this project is to create a DB to analyze the set of songs and draw different conclusions using SQL queries
###### To do this, we are asked to develop an ETL that downloads the raw information from a Udacity S3 bucket, processes the data using Spark and saves them in tables for further analysis in another S3 bucket created for this purpose.

### 2. State and justify your database schema design and ETL pipeline.  
###### After an analysis of the data, it is concluded that the best architecture to separate the information and give a better response to analytical queries is star.  
###### These are the different steps to follow:

### etl.py  

###### After loading the necessary libraries and indicating the access_key and secret_acces_key necessary to establish the connection, 4 functions are created.

#### 1. create_spark_session()

###### Construct spark object from class pyspark.sql.SparkSession

#### 2. process_song_data(spark, input_data, output_data)

###### Song processing function with 3 input parameters:

###### spark = create_spark_session() --> object from class SparkSession
###### input_data = "s3a://udacity-dend/" --> data source
###### output_data = "s3a://aws-logs-004583112324-us-west-2/data/" --> bucket S3 where the final bbdd will be hosted for analysis

###### Once all the json files of the data source (input_data) are loaded in a dataframe, we proceed to extract the columns to create the table, songs_table.  
###### Then, the songs_table table in parquet format partitioned by year and artist is written to the destination repository (output_data).  
###### To create the artist_table, we extract the relevant columns just like with song_table.  
###### Finally we write to the destination S3 the artist_table table in parquet format.  

#### 3. process_log_data(spark, input_data, output_data)

###### Log processing function with 3 input parameters  
###### spark = create_spark_session() --> object from class SparkSession
###### input_data = "s3a://udacity-dend/" --> data source
###### output_data = "s3a://aws-logs-004583112324-us-west-2/data/" -->  bucket S3 where the final bbdd will be hosted for analysis

###### Then select the log files, they are filtered by song plays and the necessary columns are extracted to create user_table and save this table in the destination S3.  
###### We create the table time_table with different columns and it is saved in destination S3.  
###### Finally, to create the songplays_table table, you need to JOIN with time_table, do SELECT to create the necessary columns, and write the resulting_table to destination S3.  

###### ... by last

#### 4. main()  
###### Main function where the values of spark, input_data and output_data.

