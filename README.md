# Movie_Recommendation_using_Spark
The project demonstrates the application of Big Data Technologies using Spark.
We used MovieLens 20M Dataset and applied ALS model to train the data.
This project is a joint effort by Dharmesh Tewari and Hina Moin Syed to satisfy subject requirements for Advance DBMS.

The IDE we used to implement the project is PyCharm. The analysis on the given dataset is performed using pySpark.
We have implemented a movie_pipeline to demonstrate all the processing involved from creating Spark Object till Recommendation.
First of all, objects are defined in create_objects.py. Then variables are declared in get_all_variables.py.
movie_pipeline.log file is created to record every activity running through the project.
movie_run_data_ingest.py defines function to ingest data over Spark which is called in movie_pipeline.py main file.
Once data is ingested, movie_run_data_preprocessing.py performs preprocessing to select required columns from the dataset and perform any clean operations.
movie_run_data_transform.py performs join and aggregate operations and using ALS model to train dataset.
The final flow comes back to main movie_pipeline and recommendation results are shown based on userID.
