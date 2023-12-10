from pyspark import SparkConf
from pyspark.sql import SparkSession
import logging
import logging.config

#Load Logging config File
logging.config.fileConfig(fname='../util/logging_to_file.conf')
logger = logging.getLogger(__name__)

def get_spark_object(envn, appName):
    try:
        logger.info(f"get_spark_object() is started. The '{envn}' envn is used.")
        if envn == 'TEST':
            master = 'local'
        else:
            master = 'yarn'
        spark_conf = SparkConf().setAppName(appName)
        if envn == 'local':
            spark_conf.setMaster('local[*]')

        spark_conf.set("spark.executer.memory", "8g")
        spark_conf.set("spark.driver.memory", "8g")

        spark_conf.set("spark.executer.memoryOverhead", "1g")
        spark_conf.set("spark.driver.memoryOverhead", "1g")

        spark_conf.set("spark.executer.extraJavaOptions", "-XX:+UseG1GC -Xmx8g -Xms8g")
        spark_conf.set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -Xmx8g -Xms8g")

        spark = SparkSession \
         .builder \
         .master(master) \
         .appName(appName) \
         .config("spark.sql.pivotMaxValues", "20000300") \
         .getOrCreate()
    except NameError as exp:
        logger.error("NameError in the method - get_spark_object(). Please check the Stack Trace. " + str(exp), exc_info=True)
        raise
    except Exception as exp:
        logger.error("Error in the method - get_spark_object(). Please check the Stack Trace. " + str(exp), exc_info=True)
    else:
        logger.info("Spark Object is created ...")
    return spark