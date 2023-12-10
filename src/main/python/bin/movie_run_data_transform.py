from pyspark.sql.functions import upper,size, countDistinct, sum
from pyspark.sql.window import Window
# from udfs import column_split_cnt

import logging
import logging.config
from validations import get_curr_date, df_count, df_top10_rec,df_print_schema
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

logger = logging.getLogger(__name__)

def movie_report(df_ratings,df_movies):

    try:
        logger.info(f"Transform - movie_report() is started...")
        df_movie_join = df_movies.join(df_ratings,(df_movies.movieId == df_ratings.movieId),'inner')
        df_movie_join = df_movie_join.drop(df_ratings['movieId'])

        # split into training and testing sets
        (training, test) = df_movies.randomSplit([.8, .2])
        # Build the recommendation model using ALS on the training data
        # Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
        als = ALS(maxIter=5, rank=4, regParam=0.01, userCol='userId', itemCol='movieId', ratingCol='rating',
                  coldStartStrategy='drop')
        # fit the ALS model to the training set
        model = als.fit(training)
        # Evaluate the model by computing the RMSE on the test data
        predictions = model.transform(test)

        evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
        rmse = evaluator.evaluate(predictions)
        # print("Root-mean-square error = " + str(rmse))
        logger.info("Root-mean-square error = " + str(rmse))

        # For single user
        single_user = test.filter(test['userId'] == 2).select(['movieId', 'userId'])
        df_top10_rec(single_user, 'single_user')
        df_print_schema(single_user, 'single_user')

        reccomendations = model.transform(single_user)



        # predictions.show()

        # ==============================================================================================
        # evaluator = RegressionEvaluator(metricName='rmse', labelCol='rating', predictionCol='prediction')
        # rmse = evaluator.evaluate(predictions)
        # print(rmse)
        #
        # # initialize the ALS model
        # als_model = ALS(userCol='userId', itemCol='movieId', ratingCol='rating', coldStartStrategy='drop')
        # # create the parameter grid
        # params = ParamGridBuilder().addGrid(als_model.regParam, [.01, .05, .1, .15]).addGrid(als_model.rank,
        #                                                                                      [10, 50, 100, 150]).build()
        # # instantiating crossvalidator estimator
        # cv = CrossValidator(estimator=als_model, estimatorParamMaps=params, evaluator=evaluator, parallelism=4)
        # best_model = cv.fit(df_movie_join)
        # model = best_model.bestModel
        #
        # final_als = ALS(maxIter=10, rank=50, regParam=0.15, userCol='userId', itemCol='movieId', ratingCol='rating',
        #                 coldStartStrategy='drop')
        # final_model = final_als.fit(training)
        # test_predictions = final_model.transform(test)
        # RMSE = evaluator.evaluate(test_predictions)
        # print(RMSE)
        # ==============================================================================================


        # df_city_join = df_city_split.join(df_fact_grp, (df_city_split.state_id == df_fact_grp.presc_state) & (df_city_split.city == df_fact_grp.presc_city), 'inner')
        # df_city_final = df_city_join.select("city", "state_name", "county_name", "population", "zip_counts", "trx_counts", "presc_counts")
        # df_rat_count = merge_df.groupBy("title").agg(count('rating').alias("rating_count"))

        # df_mov_grp = df_ratings.groupBy(df_ratings.movieId).agg(countDistinct("rating").alias("rating_count"))//37-41
        # df_movie_join = df_movies.join(df_ratings,(df_movies.movieId == df_ratings.movieId),'inner')
        # df_movie_join = df_movie_join.drop(df_ratings['movieId'])
        # df_movie_grp = df_movie_join.groupBy(df_movie_join.title).agg(countDistinct("rating").alias("rating_count"), sum("rating").alias("sum_rating_count"))
        #
        # top_10_df = df_movie_grp.orderBy(df_movie_grp['sum_rating_count'].desc()).limit(10)
        # df_movie_join1 = df_movies.join(df_movie_grp, (df_movies.title == df_movie_grp.title), 'inner')
        # df_movie_join1 = df_movie_join.drop(df_movie_grp['title'])
        # df_rat_count = df_movie_join.groupBy("title").agg(count('rating').alias("rating_count"))
        # df_movie_grp = df_movie_join.groupBy(df_movie_join.title, df_movie_join.rating).agg(countDistinct("rating").alias("rating_count"), sum("rating").alias("sum_rating_count"))
        # df_movie_final = df_movie_grp.select('userID','movieId','rating','title','genres','rating_count')

    except Exception as exp:
        logger.error("Error in the method - movie_report(). Please check the Stack Trace. " + str(exp),exc_info=True)
        raise
    else:
        logger.info("Transform - movie_report() is completed...")
    # return df_movie_final
    return reccomendations






#  merge both dataframes 80- 103
       #  merge_df = df_movies.join(df_r2, df_movies['movieId'] == df_r2['movieId'], 'inner')
       #  merge_df = merge_df.drop(df_r2['movieId'])
       #  merge_df = merge_df.select("userId",'movieId','rating','title','genres')
       #
       #  df_count(merge_df, 'merge_df')
       #  df_top10_rec(merge_df, 'merge_df')
       #
       #  df_rat_count = merge_df.groupBy("title").agg(count('rating').alias("rating_count"))6666
       #
       #  df_count(df_rat_count, 'df_rat_count')
       #  df_top10_rec(df_rat_count, 'df_rat_count')
       #
       #  final_df = merge_df.join(df_rat_count, merge_df['title']==df_rat_count['title'],'left')
       #  final_df = final_df.drop(merge_df['title'])
       #  final_df = final_df.select('userID','movieId','rating','title','genres','rating_count')
       #  final_df.show()
       #
       #
       #  threshold = 61
       #  top_rated_movies = final_df.filter(final_df['rating_count'] > threshold)
       #  # top_rated_movies.show()
       #  df_count(top_rated_movies, 'top_rated_movies')
       #  df_top10_rec(top_rated_movies, 'top_rated_movies')

        # Pivot the DataFrame
        # pivot_df = final_df.limit(10000)
        ## pivoted_df = top_rated_movies.groupBy("userID").pivot("title").agg(first(col("rating")))
        # df_count(pivoted_df, 'pivoted_df')
        # df_top10_rec(pivoted_df, 'pivoted_df')

        # similar_mov = pivoted_df.select('Grumpier Old Men').alias('similar_mov')
        # correlation = pivoted_df.corr(similar_mov, method='pearson')
        # correlation_df = correlation.select(col('Grumpier Old Men').alias('movie_title'), col('correlation'))
        # sorted_correlation_df = correlation_df.orderBy(col('correlation').desc())
        #
        # result = top_rated_movies.groupBy('title').agg(count('title').alias('title_count'), mean('rating').alias('rating_mean'))
        #
        # joined_df = sorted_correlation_df.join(result, 'title')
        # top_10_df = joined_df.orderBy(col('correlation').desc()).limit(10)
        #
        # df_count(top_10_df, 'top_10_df')
        # df_top10_rec(top_10_df, 'top_10_df')

        # df_count(result, 'result')
        # df_top10_rec(result, 'result')
        # pivot_df = final_df.groupBy("userID").pivot("title").agg(first(col("rating")))
        # pivot_df = final_df.groupBy("userID").pivot("title").agg(col("rating").first())



        # pivoted_df = top_rated_movies.groupBy("userID").pivot("title").agg(top_rated_movies["rating"].first())

        # Show the result
        # pivot_df.show()