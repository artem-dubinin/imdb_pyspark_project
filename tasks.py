from df_schemas import *
import pyspark.sql.functions as f


def ukr_movies():
    return akas_df().select('title', 'region').where(f.col('region') == 'UA')


def nineteenth_century_people():
    return name_basics_df().select('primaryName', 'birthYear').where(f.col('birthYear').between(1801, 1900))


def movies_more_than_two_hours():
    return basics_df().select('originalTitle', 'titleType', 'runtimeMinutes').where(
        (f.col('titleType') == 'movie') & (f.col('runtimeMinutes') > 120))


def names_films_characters():
    principals_new_df = principals_df().where(f.col('category').isin('actor', 'actress'))

    films_characters_df = principals_new_df.join(basics_df(), on='tconst', how='left')

    names_films_characters_df = films_characters_df.join(name_basics_df(), on='nconst', how='left').select(
        'primaryName',
        'primaryTitle',
        'characters')
    return names_films_characters_df


def top_regions_by_adult_titles():
    adult_titles = basics_df().select('tconst', 'isAdult').where(f.col('isAdult') == 1)

    titles_per_region = adult_titles.join(akas_df(), 'tconst', 'left')
    top_regions = titles_per_region.groupby('region').count().orderBy(('count'), ascending=False).dropna()
    return top_regions.limit(100)


def tv_series_episodes():
    count_series = episode_df().groupby('tconst').count()

    result = count_series.join(basics_df(), 'tconst', 'left').select('primaryTitle', 'count').orderBy('count', ascending=False)
    return result.limit(50)


def top_titles_by_decade():
    result = spark_session.createDataFrame([], t.StructType([t.StructField('primaryTitle', t.StringType(), True),
                                                             t.StructField('startYear', t.IntegerType(), True),
                                                             t.StructField('averageRating', t.IntegerType(), True),
                                                             t.StructField('numVotes', t.IntegerType(), True),
                                                             t.StructField('decade', t.IntegerType(), True)
                                                             ]))

    film_rating = basics_df().join(ratings_df(), 'tconst', 'inner').select('primaryTitle', 'startYear', 'averageRating', 'numVotes')

    year = 1870
    while year < 2030:
        rating_per_decade = film_rating.where(f.col('startYear').between(year, year + 9)) \
            .orderBy(f.col('averageRating').desc(), f.col('numVotes').desc()).limit(10).withColumn('decade', f.lit(year).cast('int'))
        result = result.union(rating_per_decade)
        year += 10
    return result


def top_titles_by_genre():
    window = Window.orderBy(f.col('averageRating').desc(), f.col('numVotes').desc()).partitionBy('genre')

    splitted_genres = basics_df().join(ratings_df(), on='tconst', how='inner').select('primaryTitle',
                                                                                      f.explode(f.split(f.col('genres'), ',')).alias('genre'),
                                                                                      'averageRating',
                                                                                      'numVotes')

    result = splitted_genres.withColumn('row_number', f.row_number().over(window)).where(f.col('row_number') <= 10)
    return result
