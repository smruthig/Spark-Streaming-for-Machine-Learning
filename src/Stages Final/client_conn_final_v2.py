from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark = SparkSession \
		.builder \
		.appName("SSML")
		.getOrCreate()
		
lines = spark \
		.readStream \
		.format("socket")
		.option("host", "localhost")
		.option("port",6100)
		.load()
		
words = lines.select(
	explode(
		split(lines.value, ";")
	).alias("word")
	
wordCounts = words.groupBy("word").count()
		
query = wordCounts \
		.writeStream \ 
		.outputMode("complete") \
		.format("console") \
		.start()
		
query.awaitTermination()