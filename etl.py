import configparser
from datetime import datetime
import os
import zipfile
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, weekofyear, hour, date_format
from pyspark.sql.types import StructType, StructField, FloatType, DecimalType, DoubleType, StringType, IntegerType, DateType, TimestampType

# get config file and parse it
config = configparser.ConfigParser()
config.read('dl.cfg')

# get access keys to connect to AWS
os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

# constants
SONG = 'song'
LOG = 'log'
SONG_DATA_ZIP = 'data/song-data.zip'
LOG_DATA_ZIP = 'data/log-data.zip'
SONG_DATA_LOCAL = 'tmp/song_data/*/*/*/*.json'
LOG_DATA_LOCAL = 'tmp/log_data/*.json'
RETURN_PATH_LOCAL = 'result/'
SONG_DATA_AWS = 's3a://udacity-dend/song_data/*/*/*/*.json'
LOG_DATA_AWS = 's3a://udacity-dend/log_data/*/*/*.json'
RETURN_PATH_AWS = config['AWS']['S3_BUCKET']

# global variables for progress/failure tracking
log_records = 0
incorrect_log_records = 0
song_records = 0
incorrect_song_records = 0


def create_spark_session():
    """ Conveniently create or get Spark session (if already exists) """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .appName("ETL using Spark") \
        .getOrCreate()
    return spark


def add_records(record_type, cnt):
    """ Add the number of records for a stage to an accumulator """
    global log_records
    global incorrect_log_records
    global song_records
    global incorrect_song_records

    if record_type == 'log_records':
        log_records += cnt
    if record_type == 'incorrect_log_records':
        incorrect_log_records += cnt
    if record_type == 'song_records':
        song_records += cnt
    if record_type == 'incorrect_song_records':
        incorrect_song_records += cnt


def to_timestamp(ts):
    """ Return a date/time in UTC from a timestamp """

    return datetime.utcfromtimestamp(ts/1000.0)


def prepare_local_data():
    """ Unzip data into /tmp folder for further processing """

    if config['PROJECT']['MODE'] == 'LOCAL':
        print("[data is extracted and persisted locally...]")
        # extract song data file
        with zipfile.ZipFile(SONG_DATA_ZIP, 'r') as zip_archive:
            zip_archive.extractall('tmp')

        # extract log data file
        with zipfile.ZipFile(LOG_DATA_ZIP, 'r') as zip_archive:
            zip_archive.extractall('tmp/log_data')
    else:
        print("[data is extracted from and persisted on AWS...]")


def get_input_output(filetype):
    """ Contextualize the input/output path and returns the correct values """

    if config['PROJECT']['MODE'] == 'LOCAL':
        # go grab the data locally
        if filetype == SONG:
            input_data = SONG_DATA_LOCAL
        if filetype == LOG:
            input_data = LOG_DATA_LOCAL

        output_data = RETURN_PATH_LOCAL
    else:
        # go grab data on AWS
        if filetype == SONG:
            input_data = SONG_DATA_AWS
        if filetype == LOG:
            input_data = LOG_DATA_AWS

        output_data = RETURN_PATH_AWS

    return input_data, output_data


def process_song_data(spark, input_data, output_data):
    """
    This function performs the following operations in order:
    - define a schema to schema-on-read json song files
    - read the files in permissive mode using Spark
    - filter incorrect entries & duplicates
    - fill in or replace empty values
    - create 2 tables from the dataset (songs & artists)
    - persist these tables as parquet files
    """

    # get filepath to song data file
    song_data = input_data

    # define the schema that matches files structure (ignoring non relevant fields)
    schema = StructType([
        StructField('artist_id', StringType()),
        StructField('artist_latitude', FloatType()),
        StructField('artist_longitude', FloatType()),
        StructField('artist_location', StringType()),
        StructField('artist_name', StringType()),
        StructField('song_id', StringType()),
        StructField('title', StringType()),
        StructField('duration', FloatType()),
        StructField('year', IntegerType()),
        StructField('_corrupt_record', StringType())
    ])

    df = spark.read \
        .schema(schema) \
        .option('mode', 'PERMISSIVE') \
        .json(song_data) \
        .cache()

    # metrics from the dataset according to schema
    add_records('song_records', df.filter(df._corrupt_record.isNull()).count())
    print("# of song records: " + str(song_records))
    add_records('incorrect_song_records', df.filter(
        df._corrupt_record.isNotNull()).count())
    print("# of incorrect song records: " + str(incorrect_song_records))
    # get correct entries
    df = df.filter(df._corrupt_record.isNull())
    # replace null with 0 for longitude/latitude
    df = df.fillna({'artist_latitude': 0, 'artist_longitude': 0})
    # what does the data look like
    #df.show(10)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration') \
                    .dropDuplicates()
    #songs_table.show(10)

    # write songs table to parquet files partitioned by year and artist
    # https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html
    songs_table.write \
        .mode('overwrite') \
        .partitionBy('year', 'artist_id') \
        .parquet(output_data + '/songs-parquet/')

    # extract columns to create artists table
    artists_table = df.select('artist_id',
                              col('artist_name').alias('name'),
                              col('artist_location').alias('location'),
                              col('artist_latitude').alias('latitude'),
                              col('artist_longitude').alias('longitude')) \
        .dropDuplicates()
    #artists_table.show(10)

    # write artists table to parquet files
    # bucketBy is not yet available but could be usefull as artist_id has a lot of unique values
    artists_table.write \
        .mode('overwrite') \
        .parquet(output_data + '/artists-parquet/')


def process_log_data(spark, input_data, output_data):
    """
    This function performs the following operations in order:
    - define a schema to schema-on-read json log files
    - read the files in permissive mode using Spark
    - filter incorrect entries & duplicates
    - filter on NextSong
    - create 2 tables from the dataset (users & time)
    - create 1 table from the dataset joined with already persisted parquet tables
    - persist these tables as parquet files
    """

    # get filepath to log data file
    log_data = input_data

    # define the schema that matches files structure (ignoring non relevant fields)
    schema = StructType([
        StructField('artist', StringType()),
        StructField('firstName', StringType()),
        StructField('gender', StringType()),
        StructField('lastName', StringType()),
        StructField('length', FloatType()),
        StructField('level', StringType()),
        StructField('location', StringType()),
        StructField('page', StringType()),
        StructField('sessionId', IntegerType()),
        StructField('song', StringType()),
        StructField('ts', FloatType()),
        StructField('userAgent', StringType()),
        StructField('userId', StringType()),
        StructField('_corrupt_record', StringType())
    ])

    # read log data file
    df = spark.read \
        .schema(schema) \
        .option('mode', 'PERMISSIVE') \
        .json(log_data) \
        .cache()

    # metrics from the dataset according to schema
    add_records('log_records', df.filter(df._corrupt_record.isNull()).count())
    print("# of log records: " + str(log_records))
    add_records('incorrect_log_records', df.filter(
        df._corrupt_record.isNotNull()).count())
    print("# of incorrect log records: " + str(incorrect_log_records))
    # get correct entries
    df = df.filter(df._corrupt_record.isNull())
    # what does the data look like
    #df.show(10)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table
    users_table = df.select(col('userId').alias('user_id'),
                            col('firstName').alias('first_name'),
                            col('lastName').alias('last_name'),
                            'gender',
                            'level') \
        .dropDuplicates()

    #users_table.show(10)

    # write users table to parquet files
    # bucketBy is not yet available but could be usefull as user_id has a lot of unique values
    users_table.write \
        .mode('overwrite') \
        .parquet(output_data + '/users-parquet/')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: to_timestamp(ts), TimestampType())
    spark.udf.register('get_timestamp', get_timestamp)

    df = df.withColumn('start_time', get_timestamp('ts')).drop('ts')

    # derive time table from timestamp
    time_table = df.select('start_time') \
        .withColumn('hour', hour(col('start_time'))) \
        .withColumn('day', dayofmonth(col('start_time'))) \
        .withColumn('week', weekofyear(col('start_time'))) \
        .withColumn('month', month(col('start_time'))) \
        .withColumn('year', year(col('start_time'))) \
        .withColumn('weekday', dayofweek(col('start_time'))) \
        .dropDuplicates()

    #time_table.show(10)

    # write time table to parquet files partitioned by year and month
    time_table.write \
        .mode('overwrite') \
        .partitionBy('year', 'month') \
        .parquet(output_data + '/time-parquet/')

    # read in song + artist data to use for songplays table
    song_df = spark.read.parquet(output_data + '/songs-parquet')
    song_df = song_df.select('song_id', 'title', 'artist_id')

    artist_df = spark.read.parquet(output_data + '/artists-parquet')
    artist_df = artist_df.select('artist_id', 'name') \
        .withColumnRenamed('artist_id', 'artist_id_dup')
    # join these 2 tables
    song_df = song_df.join(artist_df, song_df.artist_id ==
                           artist_df.artist_id_dup)
    # drop unnecessary columns
    song_df = song_df.drop('artist_id_dup')
    #song_df.show(10)

    # extract columns from joined song and log datasets to create songplays table
    songplays_df = df.select('start_time',
                             col('userId').alias('user_id'),
                             'level',
                             'song',
                             'artist',
                             col('sessionId').alias('session_id'),
                             'location',
                             col('userAgent').alias('user_agent')) \
        .dropDuplicates()

    #songplays_df.show(10)
    # https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=join
    songplays_join = songplays_df.join(song_df,
                                       ((song_df.title == songplays_df.song) &
                                        (song_df.name == songplays_df.artist)),
                                       'inner')
    # https://towardsdatascience.com/adding-sequential-ids-to-a-spark-dataframe-fa0df5566ff6
    # discard unnecessary columns & add new ones for unique Id & partition
    songplays_table = songplays_join \
        .drop('song', 'artist') \
        .withColumn('songplay_id', monotonically_increasing_id()) \
        .withColumn('year', year(col('start_time'))) \
        .withColumn('month', month(col('start_time')))

    #songplays_table.show(10)

    # write songplays table to parquet files partitioned by year and month
    # as stated in this udacity knowledge post YEAR & MONTH should be added to the table for partitioning
    # https://knowledge.udacity.com/questions/150979
    songplays_table.select(
        'songplay_id',
        'start_time',
        'year',
        'month',
        'user_id',
        'level',
        'song_id',
        'artist_id',
        'session_id',
        'location',
        'user_agent') \
        .write \
        .mode('overwrite') \
        .partitionBy('year', 'month') \
        .parquet(output_data + '/songplay-parquet')


def main():
    """
    This is the high-level ETL process that executes the following tasks:
    - get or create a Spark session for jobs submission
    - get the log & song data from an S3 bucket
    - transform the data to make them available as parquet files
    - write the data to an S3 bucket as persistence layer
    """

    spark = create_spark_session()

    # define accumulators to get info from worker nodes
    log_records = spark.sparkContext.accumulator(0, 0)
    incorrect_log_records = spark.sparkContext.accumulator(0, 0)
    song_records = spark.sparkContext.accumulator(0, 0)
    incorrect_song_records = spark.sparkContext.accumulator(0, 0)

    # extract zip files if local data processing
    prepare_local_data()

    # process song data
    input_data, output_data = get_input_output(SONG)
    process_song_data(spark, input_data, output_data)

    # process log data
    input_data, output_data = get_input_output(LOG)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
