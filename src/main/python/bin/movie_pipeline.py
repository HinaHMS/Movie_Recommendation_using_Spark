### Import all necessary modules
import os
import sys
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
import matplotlib.pyplot as plt
from pyspark.sql.functions import col, first, max
from pyspark.sql import SparkSession

import get_all_variables as gav
from create_objects import get_spark_object
from validations import get_curr_date, df_count, df_top10_rec,df_print_schema
import logging.config
from movie_run_data_ingest import load_files
from pyspark.sql.functions import count,mean
from movie_run_data_preprocessing import perform_data_clean
from movie_run_data_transform import movie_report

logging.config.fileConfig(fname='../util/logging_to_file.conf')

def main():
    try:
        logging.info("main() is started ...")

        ### Get Spark Object
        spark = get_spark_object(gav.envn,gav.appName)

        # Validate Spark Object
        get_curr_date(spark)

    ### Initiate movie_data_ingest Script
       # Load the movie file
        for file in os.listdir(gav.staging_movie):
            print("File is "+ file)
            file_dir = gav.staging_movie + '\\' + file
            print(file_dir)
            if file.split('.')[1] == 'csv':
                file_format = 'csv'
                header = gav.header
                inferSchema = gav.inferSchema
            elif file.split('.')[1] == 'parquet':
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'

        df_movies = load_files(spark = spark, file_dir = file_dir, file_format = file_format, header = header, inferSchema = inferSchema)

       # Validate run_data_ingest script for movies dataframe
        df_count(df_movies,'df_movies')
        df_top10_rec(df_movies,'df_movies')

       # Load the rating file
        for file in os.listdir(gav.staging_rating):
            print("File is "+ file)
            file_dir = gav.staging_rating + '\\' + file
            print(file_dir)
            if file.split('.')[1] == 'csv':
                file_format = 'csv'
                header = gav.header
                inferSchema = gav.inferSchema
            elif file.split('.')[1] == 'parquet':
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'

        df_ratings = load_files(spark = spark, file_dir = file_dir, file_format = file_format, header = header, inferSchema = inferSchema)

        # select userId  movieId  rating only and discard timestamp
        df_r2 = perform_data_clean(df_ratings,df_movies)
        # df_r2 = df_ratings.select("userId","movieId","rating")

       # Validate
        df_count(df_r2, 'df_r2')
        df_top10_rec(df_r2, 'df_r2')
        df_print_schema(df_r2, 'df_r2')

       #  Order by 'prediction' in descending

        df_movie_final = movie_report(df_movies,df_r2)
        df_movie_final = df_movie_final.orderBy('prediction',ascending = False)
        df_top10_rec(df_movie_final, 'df_movie_final')
        df_print_schema(df_movie_final, 'df_movie_final')

        # Extracting Movie Names
        df_movie_join = df_movie_final.join(df_movies, (df_movie_final.movieId == df_movies.movieId), 'inner')
        df_movie_join = df_movie_join.drop(df_ratings['movieId'])
        df_movie_join = df_movie_join.orderBy('prediction', ascending=False)
        df_top10_rec(df_movie_join, 'df_movie_join')
        df_print_schema(df_movie_join, 'df_movie_join')


        logging.info("movie_pipeline is Completed.")
    except Exception as exp:
        logging.error("Error in the main() method. Please check the Stack Trace to go to the respective module and fix it." + str(exp),exc_info=True)
        sys.exit(1)

### End of Part-1

if __name__ == '__main__':
    logging.info("movie_pipeline is Started ...")
    main()