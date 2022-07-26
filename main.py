from tasks import *


def run():

    """
     Running functions that return dataframes according to the task

     Writing results to the csv file with relevant titles

     Overwriting results if such file name exists

    """

    ukr_movies().coalesce(1).write.csv('Ukrainian movies', header=True, mode="overwrite")

    nineteenth_century_people().coalesce(1).write.csv('19th century people', header=True, mode="overwrite")

    movies_more_than_two_hours().coalesce(1).write.csv('Movies more than 2 hours', header=True, mode="overwrite")

    names_films_characters().coalesce(1).write.csv('Actors and their roles', header=True, mode="overwrite")

    top_regions_by_adult_titles().coalesce(1).write.csv('Top 100 regions by adult titles', header=True, mode="overwrite")

    tv_series_episodes().coalesce(1).write.csv('Top 50 biggest TV series', header=True, mode="overwrite")

    top_titles_by_decade().coalesce(1).write.csv('Top 10 titles by decade', header=True, mode="overwrite")

    top_titles_by_genre().coalesce(1).write.csv('Top 10 titles by genre', header=True, mode="overwrite")


if __name__ == "__main__":
    run()
