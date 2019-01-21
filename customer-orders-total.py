from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CustomerOrdersTotal")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    return (int(fields[0]), float(fields[2]))

#Fetching input csv file and loading to rdd as key value pairs
lines = sc.textFile("file:///SparkCourse/customer-orders.csv")
rdd = lines.map(parseLine)

# Finding totals for each customer
totalsByCustomer = rdd.reduceByKey(lambda x, y: x + y )

#Initiate action and storing in a Python object to iterate and print
results = totalsByCustomer.collect()

#Printing
for result in results:
    print(result)