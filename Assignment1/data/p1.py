# Imports
import sys
from pyspark.sql import SparkSession
import csv
import pyspark.sql.types 
import pyspark.sql.functions 
from pyspark.sql.functions import udf

def create_dataframe(filepath, format, spark):


    """
    Create a spark df given a filepath and format.
    
    :param filepath: <str>, the filepath
    :param format: <str>, the file format (e.g. "csv" or "json")
    :param spark: <str> the spark session

    :return: the spark df uploaded
    """
    spark_df = None

    if format == 'csv':
        with open(filepath) as f:
            nhis = []
            nhis_input = csv.reader(f, delimiter = ',')
            for i, eachLine in enumerate(nhis_input):
                if i == 0:
                    schema = eachLine
                else:
                    num = []
                    for n in eachLine:
                        num.append(int(n))
                    nhis.append(num)
            spark_df = spark.createDataFrame(nhis, schema)


    if format == 'json':
        with open(filepath) as f:
            brfss = []
            for eachLine in f:
                brfss.append(eval(eachLine))
            spark_df = spark.createDataFrame(brfss)
    

    return spark_df


def transform_nhis_data(nhis_df):
    """
    Transform df elements

    :param nhis_df: spark df
    :return: spark df, transformed df
    """
    transformed_df = None

    def transform_race_data(x, y):
        if y != 12:
            return 5.0
        if x == 1:
            return 1.0
        if x == 2:
            return 2.0
        if x == 3:
            return 4.0
        if x == 6:
            return 3.0
        if x == 7:
            return 3.0
        if x == 12:
            return 3.0
        if x == 16:
            return 6.0
        if x == 17:
            return 6.0
        return 5.0
    race = udf(lambda x, y: transform_race_data(x, y), pyspark.sql.types.FloatType())

    def transform_age_data(n):
        if n < 18:
            return 14.0
        if n >= 80:
            return 13.0
        if n <= 24:
            return 1.0
        a = (n - 25) // 5
        return a * 1.0
    age = udf(lambda n: transform_age_data(n), pyspark.sql.types.FloatType())

    transformed_df = nhis_df.select('SEX', 'DIBEV1', age('AGE_P').alias('_AGEG5YR'), race('MRACBPI2', 'HISPAN_I').alias('_IMPRACE'))

    return transformed_df


def calculate_statistics(joined_df):
    """
    Calculate prevalence statistics

    :param joined_df: the joined df

    :return: None

    """
    condition = lambda cond: pyspark.sql.functions.sum(pyspark.sql.functions.when(cond, 1).otherwise(0))
    race_df = joined_df.groupby('_IMPRACE').\
    agg((condition(joined_df.DIBEV1 == 1)/pyspark.sql.functions.count(pyspark.sql.functions.lit(1))).alias('prevalence statistics'))
    race_df.show()
    
    age_df = joined_df.groupby('_AGEG5YR').\
    agg((condition(joined_df.DIBEV1 == 1)/pyspark.sql.functions.count(pyspark.sql.functions.lit(1))).alias('prevalence statistics'))
    age_df.show()

    sex_df = joined_df.groupby('SEX').\
        agg((condition(joined_df.DIBEV1 == 1)/pyspark.sql.functions.count(pyspark.sql.functions.lit(1))).alias('prevalence statistics'))
    sex_df.show()
    pass


if __name__ == '__main__':

    brfss_file_arg = sys.argv[1]
    nhis_file_arg = sys.argv[2]
    save_output_arg = sys.argv[3]
    if save_output_arg == "True":
        output_filename_arg = sys.argv[4]
    else:
        output_filename_arg = None
    pass


    # Start spark session
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    brfss_df = create_dataframe(brfss_file_arg, 'json', spark)
    brfss_df.show()    
    nhis_df = create_dataframe(nhis_file_arg, 'csv', spark)
    nhis_df.show()
    joined_nhis = transform_nhis_data(nhis_df)
    joined_df = brfss_df.join(joined_nhis, ['SEX', '_AGEG5YR', '_IMPRACE'], 'left')
    joined_df = joined_df.na.drop()
    joined_df.show()
    calculate_statistics(joined_df)
    # Stop spark session 
    spark.stop()

