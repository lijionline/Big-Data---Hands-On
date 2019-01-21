from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")
movies = lines.map(lambda x: (int(x.split()[1]), 1))
movieCounts = movies.reduceByKey(lambda x, y: x + y).sortBy(lambda x : x[1])

#flipped = movieCounts.map( lambda xy: (xy[1],xy[0]) )
#sortedMovies = flipped.sortByKey()

results = movieCounts.collect()

for result in results:
    print(result)
