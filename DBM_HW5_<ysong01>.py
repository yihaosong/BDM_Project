citi = 'citibike.csv'
yellow ='yellow.csv.gz'

citibike = sc.textFile(citi,use_unicode=False)
yellowcab = sc.textFile(yellow, use_unicode=False)

list(enumerate(citibike.first().split(',')))
list(enumerate(yellowcab.first().split(',')))

def get_address(partitionId, partition):
    if partitionId == 0:
        partition.next()
    import csv
    reader = csv.reader(partition)
    for row in reader:
        if row[6] == 'Greenwich Ave & 8 Ave' and row[3].split(' ')[0] == '2015-02-01':
            (starttime, start_station_latitude, start_station_longitude) = (row[3].split(' ')[1].split('+')[0], row[7], row[8])
            yield (starttime, start_station_latitude, start_station_longitude)
greendwich_8 = citibike.mapPartitionsWithIndex(get_address)
greendwich_8.count()
greendwich_8.collect()[:5]

def get_dropoff(partitionId, partition):
    if partitionId == 0:
        partition.next()
    import csv
    reader = csv.reader(partition)
    for row in reader:
        if row[1].split(' ')[0] == '2015-02-01':
            (tpep_dropoff_datetime,dropoff_latitude,dropoff_longitude) = (row[1].split(' ')[1].split('.')[0],row[4],row[5])
            yield (tpep_dropoff_datetime,dropoff_latitude,dropoff_longitude)

dropoff_info = yellowcab.mapPartitionsWithIndex(get_dropoff)
dropoff_info.count()
dropoff_info.collect()[:5]

# The haversine formula, getting distance based on longitude and latitude
from math import radians, cos, sin, asin, sqrt
def haversine(lon1, lat1, lon2, lat2):
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    # Radius of earth in kilometers is 6371. Use 3956 for miles
    r = 3956
    return c * r

import datetime as dt
from datetime import datetime

#crossjoin the two sets, if the two sets have M and N elements, the total operation woulb be M*N.
rdd = dropoff_info.cartesian(greendwich_8).\
    filter(lambda x: (dt.datetime.strptime(x[0][0],'%H:%M:%S') - dt.datetime.strptime(x[1][0],'%H:%M:%S') >= dt.timedelta(minutes = 0))\
    and (dt.datetime.strptime(x[0][0],'%H:%M:%S') - dt.datetime.strptime(x[1][0],'%H:%M:%S') <= dt.timedelta(minutes=10))\
    and (haversine(float(x[0][1]),float(x[0][2]),float(x[1][1]),float(x[1][2])) <= 0.25)
          )
print rdd.count()