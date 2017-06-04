# load libraries
import pyspark
def mapper1(id, data):
    # skip header row
    if id == 0:
        data.next()
    import csv
    reader = csv.reader(data)
    for row in reader:
        if (row[1] != "" and row[2] != "" and row[5] !=""):
            RequestType = row[5]
            yield (RequestType,1)

def mapper2(id,data):
    if id == 0:
        data.next()
    import csv
    import datetime as dt
    reader = csv.reader(data)
    for row in reader:
        if (row[1] != "" and row[2] != ""):
            CreatedDate = dt.datetime.strptime(row[1],'%m/%d/%Y %I:%M:%S %p')
            UpdatedDate = dt.datetime.strptime(row[2],'%m/%d/%Y %I:%M:%S %p')
        if row[6] == 'Closed':
            d = UpdatedDate - CreatedDate
            d = d.total_seconds()
            RequestType = row[5]
            yield (RequestType,d)

if __name__=='__main__':

    sc = pyspark.SparkContext()
    data = sc.textFile('hdfs:///user/wchong000/project/new311.csv',use_unicode=False).cache()
    rdd1 = data.mapPartitionsWithIndex(mapper1).reduceByKey(lambda a, b: a+b)
    request_type_count = rdd1.count()
    print request_type_count
    rdd1.coalesce(1,True).saveAsTextFile('hdfs:///user/ysong01/project/nyc_request_type')
