from pyspark.sql import SparkSession
from datetime import date
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
from pyspark.sql.types import *
from pyspark.sql.functions import udf, pandas_udf, PandasUDFType

spark = SparkSession.builder.getOrCreate()
df = spark.read.json('data100000.json')
df = df.drop('_id','idplug_base','idunplug_base','track', 'zip_code')

def get_weekday(hourtime):
    """
    Devuelve el día de la semana a partir de un iterador que contiene
    una fecha con un string
    0:Lunes,...,6:Domingo
    """
    try:
        year = int(hourtime[:4])
        month = int(hourtime[5:7])
        day = int(hourtime[8:10])
        return date(year, month, day).weekday()
    
    except BaseException:
        year = int(hourtime[10:14])
        month = int(hourtime[15:17])
        day = int(hourtime[18:20])
        return date(year, month, day).weekday()

day_func = udf(lambda x: get_weekday(x), IntegerType())
df = df.withColumn('get_weekday', day_func(df['unplug_hourTime']))

def get_year(hourtime):
    """
    Devuelve el año
    """
    try:
        year = int(hourtime[:4])
        month = int(hourtime[5:7])
        day = int(hourtime[8:10])
        return date(year, month, day).year()

    except BaseException:
        year = int(hourtime[10:14])
        month = int(hourtime[15:17])
        day = int(hourtime[18:20])
        return date(year, month, day).year()
    
year_func = udf(lambda x: get_year(x), IntegerType())    
df = df.withColumn('get_year', year_func(df['unplug_hourTime']))

df2018 = df.filter(df["get_year"] == 2018)
df2018.show
df2020 = df.filter(df["get_year"] == 2020)
df2020.show

