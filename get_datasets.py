from pyspark import SparkConf, SparkContext
import os
import datetime


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
                                 datetime.datetime.strptime(elems[2], "%Y-%m-%d %H:%M").isoformat(), elems[6],
                                 elems[8], '', '', '', '', elems[9], elems[-2],
                                 str(datetime.datetime.strptime(elems[1], "%Y-%m-%d %H:%M").year - int(elems[-1])) if elems[-1] else ''])
            else:
                return ','.join([elems[0], '', datetime.datetime.strptime(elems[1], "%Y-%m-%d %H:%M:%S").isoformat(),
                                 datetime.datetime.strptime(elems[2], "%Y-%m-%d %H:%M:%S").isoformat(), elems[6],
                                 elems[8], '', '', '', '', elems[9], elems[-2],
                                 str(datetime.datetime.strptime(elems[1], "%Y-%m-%d %H:%M:%S").year - int(elems[-1])) if elems[-1] else ''])
        else:
            return ','.join([elems[0], '', datetime.datetime.strptime(elems[1], "%m/%d/%Y %H:%M").isoformat(),
                             datetime.datetime.strptime(elems[2], "%m/%d/%Y %H:%M").isoformat(), elems[6],
                             elems[8], '', '', '', '', elems[9], elems[-2],
                             str(datetime.datetime.strptime(elems[1], "%m/%d/%Y %H:%M").year - int(elems[-1])) if elems[-1] else ''])
    except:
        return ''


def map_2020_2023(elems):
     try:
        return ','.join([elems[0], elems[1], datetime.datetime.strptime(elems[2], "%Y-%m-%d %H:%M:%S").isoformat(),
                         datetime.datetime.strptime(elems[3], "%Y-%m-%d %H:%M:%S").isoformat(),
                         elems[4], elems[6], elems[8], elems[9],
                         elems[10], '', ''])
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


if __name__ == "__main__":
    conf = SparkConf().setAppName('test').setMaster('local')
    sc = SparkContext(conf=conf)
    get_data()