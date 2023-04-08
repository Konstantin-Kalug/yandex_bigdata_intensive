from pyspark import SparkConf, SparkContext
import datetime
import shutil

shutil.rmtree("clean_sources", ignore_errors=True)
def parse_stations(line):
    id_, name, latitude, longitude, dpcapacity, landmark, online_date = line.strip().split(",")
    if id_ != "id":
        yield {
            "id": id_,
            "name": name,
            "latitude": latitude,
            "longitude": longitude,
            "dpcapacity": dpcapacity,
            "landmark": landmark,
            "online_date": datetime.datetime.strptime(online_date, "%m/%d/%Y").date().isoformat()
        }

def parse_trips(line):
    trip_id, starttime, stoptime, bikeid, tripduration, from_station_id, from_station_name, to_station_id,to_station_name, usertype, gender, birthday = line.strip().split(",")
    if trip_id != "trip_id":
        yield {
            "trip_id": trip_id,
            "starttime": datetime.datetime.strptime(starttime, "%Y-%m-%d %H:%M").isoformat(),
            "stoptime": datetime.datetime.strptime(stoptime, "%Y-%m-%d %H:%M").isoformat(),
            "bikeid": bikeid,
            "tripduration": tripduration,
            "from_station_id": from_station_id,
            "from_station_name": from_station_name,
            "to_station_id": to_station_id,
            "to_station_name": to_station_name,
            "usertype": usertype,
            "gender": gender,
            "birthday": birthday
        }

def map_trips(line):
    from_station_id, (trip, from_station) = line
    return {
        "trip_id": trip["trip_id"],
        "starttime": trip["starttime"],
        "stoptime": trip["stoptime"],
        "bikeid": trip["bikeid"],
        "tripduration": trip["tripduration"],
        "from_station_id": from_station_id,
        "from_station_name": trip["from_station_name"],
        "to_station_id": trip["to_station_id"],
        "to_station_name": trip["to_station_name"],
        "usertype": trip["usertype"],
        "gender": trip["gender"],
        "birthday": trip["birthday"],
        "from_latitude": from_station["latitude"] if from_station else None,
        "from_longitude": from_station["longitude"] if from_station else None,
        "from_dpcapacity": from_station["dpcapacity"] if from_station else None,
        "from_landmark": from_station["landmark"] if from_station else None,
        "from_online_date": from_station["online_date"] if from_station else None
    }

def finalise(line):
    to_station_id, (trip_from, to_station) = line
    return ",".join([str(x) for x in[
        trip_from["trip_id"],
        None, #type unknown
        trip_from["starttime"],
        trip_from["stoptime"],
        trip_from["from_station_name"],
        trip_from["to_station_name"],
        trip_from["from_latitude"],
        trip_from["from_longitude"],
        to_station["latitude"] if to_station else None,
        to_station["longitude"] if to_station else None,
        trip_from["usertype"],
        trip_from["gender"],
        trip_from["birthday"]
    ]])


def get_data():
    directory = "data/"
    stations = sc.textFile(f"{directory}Divvy_Stations_Trips_2013/Divvy_Stations_2013.csv").flatMap(parse_stations)
    trips = sc.textFile(f"{directory}Divvy_Stations_Trips_2013/Divvy_Trips_2013.csv").flatMap(parse_trips)

    stations_by_id = stations.keyBy(lambda x: x["id"])
    trips_by_from = trips.keyBy(lambda x: x["from_station_id"])
    trips_from = trips_by_from.leftOuterJoin(stations_by_id) \
                              .map(map_trips)

    trips_from_to = trips_from.keyBy(lambda x: x["to_station_id"])
    final = trips_from_to.leftOuterJoin(stations_by_id) \
                         .map(finalise) \
                         .coalesce(1) \
                         .saveAsTextFile(f"{directory}clean_sources/2013")


if __name__ == "__main__":
    conf = SparkConf().setAppName("toclean").setMaster("local")
    sc = SparkContext(conf=conf)
    get_data()
