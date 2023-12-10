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
Following are some of the sceenshots of our project:

![WhatsApp Image 2023-12-06 at 8 49 51 PM](https://github.com/HinaHMS/Movie_Recommendation_using_Spark/assets/143968843/4f4f098a-bbcf-414b-9b88-fa1a40183500)
![WhatsApp Image 2023-12-06 at 9 28 46 PM](https://github.com/HinaHMS/Movie_Recommendation_using_Spark/assets/143968843/6671f6bb-ea12-45a5-9ea4-2a4bfd9804b9)
![WhatsApp Image 2023-12-06 at 9 29 19 PM](https://github.com/HinaHMS/Movie_Recommendation_using_Spark/assets/143968843/e663b9b5-e67b-4772-9e0a-b06af3a8a954)
![WhatsApp Image 2023-12-06 at 11 35 18 PM](https://github.com/HinaHMS/Movie_Recommendation_using_Spark/assets/143968843/86cc1f97-71e0-4b8f-be3f-efe15e088204)
![WhatsApp Image 2023-12-06 at 11 35 18 PM (1)](https://github.com/HinaHMS/Movie_Recommendation_using_Spark/assets/143968843/b1feed29-1f9e-419b-89f5-363ee2fbd0af)
![WhatsApp Image 2023-12-07 at 1 16 05 AM](https://github.com/HinaHMS/Movie_Recommendation_using_Spark/assets/143968843/25fea767-560a-462a-9a73-5b3eb7c1dfa8)
![WhatsApp Image 2023-12-07 at 1 16 21 AM](https://github.com/HinaHMS/Movie_Recommendation_using_Spark/assets/143968843/21191d74-2b4c-47b0-8c97-b29453f09199)
![WhatsApp Image 2023-12-07 at 1 17 11 AM](https://github.com/HinaHMS/Movie_Recommendation_using_Spark/assets/143968843/7f3c687e-736a-4090-993c-f90f24bffe35)
![WhatsApp Image 2023-12-07 at 1 18 28 AM](https://github.com/HinaHMS/Movie_Recommendation_using_Spark/assets/143968843/d9ae014d-82e3-4c22-a783-44161ef64666)
![WhatsApp Image 2023-12-07 at 1 19 22 AM](https://github.com/HinaHMS/Movie_Recommendation_using_Spark/assets/143968843/af94346a-7389-449b-adaf-00534fa721d4)
