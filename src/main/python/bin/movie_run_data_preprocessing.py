import logging
import logging.config
from pyspark.sql.functions import upper, col, isnan, count, when

# Load the Logging Configuration File
logging.config.fileConfig(fname='../util/logging_to_file.conf')
logger = logging.getLogger(__name__)

def perform_data_clean(df1,df2):

    #1 Select only required Columns

    try:
        logger.info(f"perform_data_clean() is started ...")
        df_rating_sel = df1.select(df1.userId,
                                 df1.movieId,
                                 df1.rating)

    # Check and clean all the Null / Nan Values
        df_rating_sel.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df_rating_sel.columns]).show()
        df2.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df2.columns]).show()

    except Exception as exp:
        logger.error("Error in the method - spark_curr_date(). Please check the Stack Trace. " + str(exp),exc_info=True)
        raise
    else:
        logger.info("perform_data_clean() is completed...")
    return df_rating_sel