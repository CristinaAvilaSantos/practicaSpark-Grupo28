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
#df.show()

def get_year(hourtime):
    try:
        year = int(hourtime[:4])
        return year

    except BaseException:
        year = int(hourtime[10:14])
        return year
    
year_func = udf(lambda x: get_year(x), IntegerType())    
df = df.withColumn('get_year', year_func(df['unplug_hourTime']))

df2018 = df.filter(df["get_year"] == 2018)
#df2018.show()
df2020 = df.filter(df["get_year"] == 2020)
#df2020.show()

estaciones20 = df2020.groupBy(df2020['idunplug_station']).count()
estaciones18 = df2018.groupBy(df2018['idunplug_station']).count()

weekday18 = df2018.groupBy('get_weekday').count()
weekday20 = df2020.groupBy('get_weekday').count()

def tablas_edades(df):  #crea tablas para cada año y para cada rango de edad,  y las almacena en lista_tablas
    lista_tablas = []
    for i in range(7):
        tabla = df.filter(df['ageRange'] == i)                 #edades2018_0 = df2018.filter(df2018['ageRange'] == 0)
        tabla = tabla.groupBy(tabla['get_weekday']).count()    #edades2018_0 = edades2018_0.groupBy(edades2018_0['get_weekday']).count() saca el recuento por                                                                   dias de la semana
        lista_tablas.append(tabla)
    return lista_tablas

def muestra_tablas_edades(df): #muestra las tablas 
    tablas = tablas_edades(df)
    for i in range(len(tablas)):
        tablas[i].show()
        
muestra_tablas_edades(df2018)
muestra_tablas_edades(df2020)
