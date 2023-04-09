from pyspark import SparkConf, SparkContext
import datetime
from math import acos, sin, cos, floor

def get_shortest_distance(lat1, lon1, lat2, lon2):
    return acos(sin(lat1) * sin(lat2) + cos(lat1) * cos(lat2) * cos(lon2 - lon1)) * 6371

def parse_table1(line):
    id_, type_, startt, endt, startn, endn, startlt, startln, endlt, endln, member, gender, age, bikeid = line.split(",")
    return (bikeid, 1)

def parse_table2(line):
    id_, type_, startt, endt, startn, endn, startlt, startln, endlt, endln, member, gender, age, bikeid = line.split(",")
    V_AVG = 12/3.6/1000
    startt = datetime.datetime.strptime(startt, '%Y-%m-%dT%H:%M:%S')
    endt = datetime.datetime.strptime(endt, '%Y-%m-%dT%H:%M:%S')
    duration = endt - startt
    #return (bikeid, get_shortest_distance(int(startlt), int(startln), int(endlt), int(endln))) через координаты
    return (bikeid, V_AVG*duration.total_seconds())

def parse_table3(line):
    id_, type_, startt, endt, startn, endn, startlt, startln, endlt, endln, member, gender, age, bikeid = line.split(",")
    return (bikeid, startt[0:7])

def calculate():
    unique_by_year = []
    for y in range(2013, 2019 + 1):
        df = sc.textFile(f"clean_sources/{y}").map(parse_table1) \
                                              .reduceByKey(lambda a, b: a + b) \
                                              .map(lambda x: (None, 1)) \
                                              .reduceByKey(lambda a, b: a + b) \
                                              .collect()
        unique_by_year.append((y, df[0][1]))
    print("Уникальные по годам:", unique_by_year)
    cum_repairs = []
    REPAIR_LIM = 1000
    REPAIR_PRICE = 1000
    BIKE_PRICE = 1000
    big = sc.parallelize([])
    for y in range(2013, 2019 + 1):
        df = sc.textFile(f"clean_sources/{y}").map(parse_table2) \
                                              .reduceByKey(lambda a, b: a + b) \
                                              
        big = big.union(df)
        analyse = big.map(lambda x: (None, floor(x[1] / REPAIR_LIM))) \
                     .reduceByKey(lambda a, b: a + b) \
                     .collect()
        cum_repairs.append((y, analyse[0][1]))
    print("Накопительное значение ремонтов по годам: ", cum_repairs)
    cum = 0
    repairs = []
    for y, s in cum_repairs:
        repairs.append((y, s - cum))
        cum += s
    print("Значение ремонтов по годам: ", repairs)
    repair_prices = [(y, x * REPAIR_PRICE) for y, x in repairs]
    print("Затраты на ремонт по годам: ", repair_prices)
    print("Средние затраты по годам: ", sum([x for y, x in repair_prices]) / len(repair_prices))
    big = sc.parallelize([])
    for y in range(2013, 2019 + 1):
        df = sc.textFile(f"clean_sources/{y}").map(parse_table3)
        big = big.union(df)
    
    unique = big.reduceByKey(lambda a, b: a if a < b else b) \
                .map(lambda x: (x[1], 1)) \
                .reduceByKey(lambda a, b: a + b) \
                .collect()
    unique_by_year2 = []
    for y in range(2013, 2019 + 1):
        summa = 0
        for d, x in unique:
            if str(y) in d:
                summa += x
        unique_by_year2.append((y, summa))
    print("Уникальные по месяцам:", unique)
    print("Уникальные по годам:", unique_by_year2)
    avg_bike = sum([x for y, x in unique_by_year2]) / len(unique_by_year2)
    print("Среднее уникальное по годам: ", avg_bike)
    print("Средние затраты на велосипеды: ", avg_bike*BIKE_PRICE)


if __name__ == "__main__":
    conf = SparkConf().setAppName('test').setMaster('local')
    sc = SparkContext(conf=conf)
    calculate()