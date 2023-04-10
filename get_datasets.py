from pyspark import SparkConf, SparkContext
import os
import datetime
import shutil


def filter_first_2013_2019(line):
    try:
        first_elem = int(line[0])
        return True
    except:
        return False


def filter_first_2020_2023(line):
    if line[0] == 'ride_id':
        return False
    return True


def del_quotation_marks(elems):
    for i in range(len(elems)):
        if '"' in elems[i]:
            elems[i] = elems[i][1:-1]
    return elems


def map_2013_2019(elems):
    try:
        if '/' not in elems[1]:
            if len(elems[1].split()[1]) != 8:
                return ','.join([elems[0], '', datetime.datetime.strptime(elems[1], "%Y-%m-%d %H:%M").isoformat(),
                                 datetime.datetime.strptime(elems[2], "%Y-%m-%d %H:%M").isoformat(), elems[-6],
                                 elems[-4], '', '', '', '', elems[-3], elems[-2],
                                 str(datetime.datetime.strptime(elems[1], "%Y-%m-%d %H:%M").year - int(elems[-1])) if elems[-1] else '', elems[3]])
            else:
                return ','.join([elems[0], '', datetime.datetime.strptime(elems[1], "%Y-%m-%d %H:%M:%S").isoformat(),
                                 datetime.datetime.strptime(elems[2], "%Y-%m-%d %H:%M:%S").isoformat(), elems[-6],
                                 elems[-4], '', '', '', '', elems[-3], elems[-2],
                                 str(datetime.datetime.strptime(elems[1], "%Y-%m-%d %H:%M:%S").year - int(elems[-1])) if elems[-1] else '', elems[3]])
        else:
            return ','.join([elems[0], '', datetime.datetime.strptime(elems[1], "%m/%d/%Y %H:%M").isoformat(),
                             datetime.datetime.strptime(elems[2], "%m/%d/%Y %H:%M").isoformat(), elems[-6],
                             elems[-4], '', '', '', '', elems[-3], elems[-2],
                             str(datetime.datetime.strptime(elems[1], "%m/%d/%Y %H:%M").year - int(elems[-1])) if elems[-1] else '', elems[3]])
    except:
        return ''


def map_2020_2023(elems):
     try:
        return ','.join([elems[0], elems[1], datetime.datetime.strptime(elems[2], "%Y-%m-%d %H:%M:%S").isoformat(),
                         datetime.datetime.strptime(elems[3], "%Y-%m-%d %H:%M:%S").isoformat(),
                         elems[4], elems[6], elems[8], elems[9],
                         elems[10], elems[11], elems[12], '', '', ''])
     except:
         return ''


def get_data():
    directory ='sources/'
    # 2013 - 2019
    for i in range(2013, 2020):
        files = None
        try:
            for filename in os.listdir(directory):
                if str(i) not in filename:
                    continue
                file = sc.textFile(os.path.join(directory, filename)) \
                    .map(lambda line: line.split(',')) \
                    .map(del_quotation_marks) \
                    .filter(filter_first_2013_2019)
                if files is None:
                    files = file
                else:
                    files = files.union(file)
            res = files.map(map_2013_2019).filter(lambda x: x != '')
            res.coalesce(1).saveAsTextFile(f'clean_sources/{i}')
        except:
            pass
    # 2020 - 2023
    for i in range(2020, 2024):
        files = None
        try:
            for filename in os.listdir(directory):
                if str(i) not in filename:
                    continue
                file = sc.textFile(os.path.join(directory, filename)) \
                    .map(lambda line: line.split(',')) \
                    .map(del_quotation_marks) \
                    .filter(filter_first_2020_2023)
                if files is None:
                    files = file
                else:
                    files = files.union(file)
            res = files.map(map_2020_2023).filter(lambda x: x != '')
            res.coalesce(1).saveAsTextFile(f'clean_sources/{i}')
        except:
            pass

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
        try:
            df = sc.textFile(f"clean_sources/{y}").map(mapper_tr)
            df_from = df.keyBy(lambda x: x["start_name"])
            inter = df_from.leftOuterJoin(stations_key) \
                .map(mapper_inter)
            df_to = inter.keyBy(lambda x: x["end_name"])
            final = df_to.leftOuterJoin(stations_key) \
                .map(finalise) \
                .coalesce(1) \
                .saveAsTextFile(f"binded_data/{y}")
        except:
            pass


def unity():
    for i in range(2013, 2020):
        os.rename(f'binded_data/{i}', f'data/src/{i}')
    for i in range(2020, 2024):
        os.rename(f'clean_sources/{i}', f'data/src/{i}')
    shutil.rmtree('binded_data')
    shutil.rmtree('clean_sources')


if __name__ == "__main__":
    conf = SparkConf().setAppName('test').setMaster('local')
    sc = SparkContext(conf=conf)
    get_data()
    bind_data()
    unity()
