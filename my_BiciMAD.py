from pyspark.sql import SparkSession
from datetime import date
import matplotlib.pyplot as plt
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
from pyspark.sql.types import *
from pyspark.sql.functions import udf, pandas_udf, PandasUDFType

spark = SparkSession.builder.getOrCreate()
df_original = spark.read.json('data100000.json')
df = df_original.drop('_corrupt_record','_id','idplug_base','idunplug_base','track', 'zip_code')
df = df.where('user_type!= 3')
df = df.where('ageRange!= 0')
porcentaje_optimo = df.count() * 100 / df_original.count()
print('Nos quedamos con un', porcentaje_optimo, '% de los datos, ya que el resto no nos aportan informacion acerca del estudio que estamos realizando')

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
df1 = df.withColumn('get_weekday', day_func(df['unplug_hourTime']))

def get_year(hourtime):
    try:
        year = int(hourtime[:4])
        return year

    except BaseException:
        year = int(hourtime[10:14])
        return year


year_func = udf(lambda x: get_year(x), IntegerType())    
df2 = df1.withColumn('get_year', year_func(df1['unplug_hourTime']))
df2.show()

df2018 = df2.filter(df2['get_year'] == 2018)
df2018.show()
df2020 = df2.filter(df2['get_year'] == 2020)
df2020.show()


weekday18 = df2018.groupBy('get_weekday').count()
weekday20 = df2020.groupBy('get_weekday').count()
tablap18 = weekday18.toPandas()
d18 = dict([(a, b) for a,b in zip(tablap18['get_weekday'], tablap18['count'])])
tablap20 = weekday20.toPandas()
d20 = dict([(a, b) for a,b in zip(tablap20['get_weekday'], tablap20['count'])])

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
        
def pasar_Pandas(df):
    lista_pandas = []
    for i in range(len(tablas_edades(df))):
        tablap = tablas_edades(df)[i].toPandas()
        lista_pandas.append(tablap)
    return lista_pandas

def pasar_diccionario(df):  #lista de diccionarios. Cada diccionario es para una franja de edad e indica el dia de la semana y el número de usos ese dia.
    diccionarios = []
    for i in range(len(pasar_Pandas(df))):
        d = dict([(a, b) for a,b in zip(pasar_Pandas(df)[i]['get_weekday'], pasar_Pandas(df)[i]['count'])])
        diccionarios.append(d)
    return diccionarios

lisdicc18 = pasar_diccionario(df2018)
lisdicc20 = pasar_diccionario(df2020)

print(d18)
print(lisdicc18)
print(d20) 
print(lisdicc20)

#muestra_tablas_edades(df2018)
#muestra_tablas_edades(df2020)

def to_porcentaje(tot, lis):
    p = []
    for i in range(len(lis)):
        t = []
        for j in range(7):
            a = 100*lis[i][j]/tot[j]
            t.append(a)
        p.append(t)
    return p

def plotear(x):
    plt.figure()
    for i in range(7):
        edad = [1,2,3,4,5,6]
        y = []
        for j in range(len(x)):
            day = x[j][i]
            y.append(day)
        plt.subplot()
        plt.xlabel('Grupos de edad')
        plt.ylabel('Tanto por ciento de uso')
        plt.title('Día de la semana %d:' %i)
        plt.bar(edad, y)
        plt.show()
print('2018')
plotear(to_porcentaje(d18, lisdicc18))
print('2020')
plotear(to_porcentaje(d20, lisdicc20))

def unir_laborables(x):
    lab = []
    for i in range(len(x)): #la i recorre cada diccionario
        lab.append(x[i][0] + x[i][1] + x[i][2] + x[i][3])
    return lab
    
def unir_fds(x):
    fds = []
    for i in range(len(x)): #la i recorre cada diccionario
        fds.append(x[i][4] + x[i][5] + x[i][6])
    return fds

def suma(x):
    s= []
    l = unir_laborables(x)
    f = unir_fds(x)
    for i in range(len(x)):
        s.append(l[i]+f[i])
    return s

def to_porcentajelab(x):
    p = []
    for i in range(len(x)):
        a = 100*unir_laborables(x)[i]/suma(x)[i]
        p.append(a)
    return p
    
def to_porcentajefds(x):
    q = []
    for i in range(len(x)):
        b = 100*unir_fds(x)[i]/suma(x)[i]
        q.append(b)
    return q

def plotear2(x):
    plt.figure()
    for i in range(6):
        daytype = ['lab', 'fds'] 
        y = [to_porcentajelab(x)[i], to_porcentajefds(x)[i]]
        plt.subplot()
        plt.xlabel('Tipo de dia')
        plt.ylabel('Tanto por ciento de uso')
        plt.title('Grupo de edad %d:' %(i+1))
        plt.bar(daytype, y)
        plt.show()
        
print('2018')
plotear2(lisdicc18)
print('2020')
plotear2(lisdicc20)
