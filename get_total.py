from pyspark import SparkContext, SparkConf

from random import random
FILES = [
"201801_Usage_Bicimad.json",
"201802_Usage_Bicimad.json",
"201803_Usage_Bicimad.json",
"201804_Usage_Bicimad.json",
"201805_Usage_Bicimad.json",
"201806_Usage_Bicimad.json",
"201807_Usage_Bicimad.json",
"201808_Usage_Bicimad.json",
"201809_Usage_Bicimad.json",
"201810_Usage_Bicimad.json",
"201811_Usage_Bicimad.json",
"201812_Usage_Bicimad.json",
"202001_movements.json",
"202002_movements.json",
"202003_movements.json",
"202004_movements.json",
"202005_movements.json",
"202006_movements.json",
"202007_movements.json",
"202008_movements.json",
"202009_movements.json",
"202010_movements.json",
"202011_movements.json",
"202012_movements.json"]


def init_sc(num):
    conf = SparkConf().setAppName(f'Bicimad {num}')
    sc = SparkContext(conf=conf)
    return sc
    
def main(sc, num):
    rddlst = []
    for f in FILES:
        rddlst.append(sc.textFile(f'hdfs:///user/crisavil/{f}'))

    rdd = sc.union(rddlst)
    with open("totalDatos.json", 'w') as fout:
        for data in rdd:
            fout.write(f'{data}\n')

if __name__ == "__main__":
    import sys
    with init_sc() as sc:
        main(sc)
