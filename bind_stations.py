from pyspark import SparkConf, SparkContext
import datetime
import os

def mapper_st(line):
    info = line.split(",")
    if info[0].isnumeric() and info[1] != "" and info[2] != "" and info[3] != "":
        yield (info[0], (info[1], info[2], info[3]))

def mapper_tr(line):
    id_, type_, started_at, ended_at, start_name, end_name, start_lat, start_lng, end_lat, end_lng, member_casul, gender, age, bikeid = line.split(",")
    return {
        "id": int(id_),
        "type": type_,
        "started_at": started_at,
        "ended_at": ended_at,
        "start_name": start_name,
        "end_name": end_name,
        "member_casul": member_casul,
        "gender": gender,
        "age": age,
        "bikeid": bikeid
    }

def mapper_inter(line):
    from_name, (data, station) = line
    return {
        "id": data["id"],
        "type": data["type"],
        "started_at": data["started_at"],
        "ended_at": data["ended_at"],
        "start_name": data["start_name"],
        "end_name": data["end_name"],
        "start_lat": station["latitude"] if station else "",
        "start_lng": station["longitude"] if station else "",
        "member_casul": data["member_casul"],
        "gender": data["gender"],
        "age": data["age"],
        "bikeid": data["bikeid"]
    }

def mapper_st_final(x):
    return {
        "name": x[1][0],
        "latitude": x[1][1],
        "longitude": x[1][2]
    }

def finalise(line):
    to_name, (data, station) = line
    return ",".join([str(x) for x in [
        data["id"],
        data["type"],
        data["started_at"],
        data["ended_at"],
        data["start_name"],
        data["end_name"],
        data["start_lat"],
        data["start_lng"],
        station["latitude"] if station else "",
        station["longitude"] if station else "",
        data["member_casul"],
        data["gender"],
        data["age"],
        data["bikeid"]
    ]])

def bind_data():
    # собрать данные о станциях
    stations = sc.textFile("stations").flatMap(mapper_st) \
                                      .reduceByKey(lambda a, b: a) \
                                      .map(mapper_st_final)
    stations_key = stations.keyBy(lambda x: x["name"])

    for y in range(2013, 2019 + 1):
        df = sc.textFile(f"clean_sources/{y}").map(mapper_tr)
        
        df_from = df.keyBy(lambda x: x["start_name"])
        inter = df_from.leftOuterJoin(stations_key) \
                       .map(mapper_inter)
        
        df_to = inter.keyBy(lambda x: x["end_name"])
        final = df_to.leftOuterJoin(stations_key) \
                     .map(finalise) \
                     .coalesce(1) \
                     .saveAsTextFile(f"binded_data/{y}")




if __name__ == "__main__":
    conf = SparkConf().setAppName('test').setMaster('local')
    sc = SparkContext(conf=conf)
    bind_data()
