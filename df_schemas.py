from pyspark import *
from pyspark.sql import *
import pyspark.sql.types as t


spark_session = (SparkSession.builder
                 .master("local")
                 .appName("task app")
                 .config(conf=SparkConf())
                 .getOrCreate()
                 )


def read_df(path, **kwargs):

    """Reading dataframe from csv file

    Args:
        path(str): path to the dataset

    Returns:
        dataframe (Pyspark df) with headers
    """

    my_df = spark_session.read.csv(path,
                                   sep=r'\t',
                                   header=True,
                                   nullValue='\\N'
                                   )
    return my_df


def akas_df():
    akas_schema_csv = t.StructType([t.StructField('titleId', t.StringType(), True),
                               t.StructField('ordering', t.IntegerType(), True),
                               t.StructField('title', t.StringType(), True),
                               t.StructField('region', t.StringType(), True),
                               t.StructField('language', t.StringType(), True),
                               t.StructField('types', t.StringType(), True),
                               t.StructField('attributes', t.StringType(), True),
                               t.StructField('isOriginalTitle', t.IntegerType(), True)
                               ])

    return read_df('Datasets/title.akas.tsv', shema=akas_schema_csv).withColumnRenamed("titleId", "tconst")


def basics_df():
    basics_schema_csv = t.StructType([t.StructField('tconst', t.StringType(), True),
                               t.StructField('titleType', t.StringType(), True),
                               t.StructField('primaryTitle', t.StringType(), True),
                               t.StructField('originalTitle', t.StringType(), True),
                               t.StructField('isAdult', t.IntegerType(), True),
                               t.StructField('startYear', t.IntegerType(), True),
                               t.StructField('endYear', t.IntegerType(), True),
                               t.StructField('runtimeMinutes', t.IntegerType(), True),
                               t.StructField('genres', t.StringType(), True)
                               ])

    return read_df('Datasets/basics.tsv', shema=basics_schema_csv)


def crew_df():
    crew_schema_csv = t.StructType([t.StructField('tconst', t.StringType(), True),
                               t.StructField('directors', t.StringType(), True),
                               t.StructField('writers', t.StringType(), True),
                               ])

    return read_df('Datasets/crew.tsv', shema=crew_schema_csv)


def episode_df():
    episode_schema_csv = t.StructType([t.StructField('tconst', t.StringType(), True),
                               t.StructField('parentTconst', t.StringType(), True),
                               t.StructField('seasonNumber', t.IntegerType(), True),
                               t.StructField('episodeNumber', t.IntegerType(), True)
                               ])

    return read_df('Datasets/episode.tsv', shema=episode_schema_csv).withColumnRenamed('tconst', 'Id').withColumnRenamed('parentTconst', 'tconst')


def principals_df():
    principals_schema_csv = t.StructType([t.StructField('tconst', t.StringType(), True),
                               t.StructField('ordering', t.IntegerType(), True),
                               t.StructField('nconst', t.StringType(), True),
                               t.StructField('category', t.StringType(), True),
                               t.StructField('job', t.StringType(), True),
                               t.StructField('characters', t.StringType(), True)
                               ])

    return read_df('Datasets/principals.tsv', shema=principals_schema_csv)


def ratings_df():
    ratings_schema_csv = t.StructType([t.StructField('tconst', t.StringType(), True),
                               t.StructField('averageRating', t.FloatType(), True),
                               t.StructField('numVotes', t.IntegerType(), True)
                               ])

    return read_df('Datasets/ratings.tsv', shema=ratings_schema_csv)


def name_basics_df():
    name_basics_schema_csv = t.StructType([t.StructField("nconst", t.StringType(), False),
                               t.StructField("primaryName", t.StringType(), False),
                               t.StructField("birthYear", t.IntegerType(), False),
                               t.StructField("deathYear", t.IntegerType(), True),
                               t.StructField("primaryProfession", t.StringType(), True),
                               t.StructField("knownForTitles", t.StringType(), True)
                               ])

    return read_df('Datasets/name.basics.tsv', shema=name_basics_schema_csv)
