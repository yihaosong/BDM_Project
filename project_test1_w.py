import datetime
import operator
import os
import sys
import time
import pyspark
import matplotlib
import matplotlib.pyplot as plt
#%matplotlib inline

#import seaborn as sns
#sns.set(style="whitegrid")

import pandas as pd


def zones_rtree(shapeFile):
    import rtree
    import fiona.crs
    import geopandas as gpd
    index = rtree.Rtree()
    neighborhood = gpd.read_file(shapeFile).to_crs(fiona.crs.from_epsg(2263))
    for idx, geometry in enumerate(neighborhood.geometry):
        index.insert(idx, geometry.bounds)
    return (index, neighborhood)

def match_zone(p, index, neighborhood):
    match = index.intersection((p.x, p.y, p.x, p.y))
    for idx in match:
        if any(map(lambda x: x.contains(p), neighborhood.geometry[idx])):
            return neighborhood['neighborhood'][idx]
    return -1

def restaurantMapper(partitions):
    import csv
    import pyproj
    import shapely.geometry as geom
    proj = pyproj.Proj(init='epsg:2263', preserve_units=True)
    index, neighborhood = zones_rtree('hdfs:///user/wchong000/project/neighborhoods.geojson')
    reader = csv.reader(partitions)
    for row in reader:
        if all((row[1], row[2])):
            location = geom.Point(proj(float(row[2]), float(row[1])))
            restaurant_zones = match_zone(location, index, neighborhood)
            if restaurant_zones != -1:
                yield(str(restaurant_zones), 1)

def heatComplaintMapper(partitions):
    import csv
    import pyproj
    import shapely.geometry as geom
    proj = pyproj.Proj(init='epsg:2263', preserve_units=True)
    index, neighborhood = zones_rtree('hdfs:///user/wchong000/project/neighborhoods.geojson')
    reader = csv.reader(partitions)
    next(reader)
    for row in reader:
        #print (row[53])
        location = geom.Point(proj(float(row[52]), float(row[51])))
        zone = match_zone(location, index, neighborhood)
        if zone != -1:
            yield (str(zone), 1)

if __name__=='__main__':

    sc = pyspark.SparkContext()

    restaurant_location = sc.textFile('hdfs:///user/wchong000/project/NYCRestaurantsLocations.csv').cache()
    restaurant_count = restaurant_location.mapPartitions(restaurantMapper).reduceByKey(lambda a, b: a+b)
    output = sorted(restaurant_count.collect())
    list(output[0:10])

    readFile = sc.textFile('hdfs:///user/wchong000/project/new311.csv')
    filterData = readFile.filter(lambda x: not x.startswith('Unique Key') and x!= '')
    heatComplaint = filterData.mapPartitions(heatComplaintMapper).reduceByKey(lambda a, b: a+b)
    #callmapper = sc.textFile('/Volumes/WORKSPACE/BDM-project/new311.csv')
    result = heatComplaint.join(restaurant_count)
    fin_result = result.collect()
    list(fin_result[0:10])

    #joinResult.saveAsTextFile('hdfs:///user/wchong000/project/result.txt')
