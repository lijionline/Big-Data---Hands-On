from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CustomerOrdersTotal")
sc = SparkContext(conf = conf)

#Parsing each line from csv and returning the object
def parseLine(line):
    fields = line.split(',')
    return (int(fields[0]), float(fields[2]))

#Fetching input csv file and loading to rdd as key value pairs
lines = sc.textFile("file:///SparkCourse/customer-orders.csv")
rdd = lines.map(parseLine)

# Finding totals for each customer and sorting
totalsByCustomerSorted = rdd.reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[1])

#Sorting by the amount spend => changed the key value pair and now the amount is the key
#totalsByCustomerSorted = totalsByCustomer.map(lambda x: (x[1], x[0])).sortByKey()
#totalsByCustomerSorted = totalsByCustomer.sortBy(lambda x: x[1])

#Initiate action and storing in a Python object to iterate and print
results = totalsByCustomerSorted.collect()

#Printing
for result in results:
    print("%i  $%.2f" % (result[0], result[1]))
