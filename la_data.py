# load libraries
import pyspark

def mapper1(id, data):
    # skip header row
    if id == 0:
        data.next()
    import csv
    reader = csv.reader(data)
    for row in reader:
        if row[6] == 'Closed':
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

    # load pyspark
    sc = pyspark.SparkContext()
    la_data = sc.textFile('hdfs:///user/wchong000/project/MyLA311_Service_Request_Data_2016.csv',use_unicode=False).cache()

    # Get the total calls for la 311 services
    total_rows = la_data.count()
    print total_rows

    # Get the number of calls that were closed
    rdd1 = la_data.mapPartitionsWithIndex(mapper1)
    closed_rows = rdd1.count()
    print closed_rows

    # Count the numbers of 311 services based on different service type
    rdd1 = rdd1.reduceByKey(lambda x,y: x+y)
    rdd1.coalesce(1,True).saveAsTextFile('hdfs:///user/ysong01/project/category_count')

    # sum the total waiting time to closed a 311 services for each category 
    rdd2 = la_data.mapPartitionsWithIndex(mapper2).reduceByKey(lambda x,y: x+y)

    # Get the average waiting time for each category
    rdd3 = rdd1.join(rdd2).map(lambda (cat,(s,c)): (cat, c/s)).sortByKey()

    # output
    rdd3.coalesce(1,True).saveAsTextFile('hdfs:///user/ysong01/project/la_data_avg_waittime')