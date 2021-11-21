from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql.context import SQLContext
from pyspark.sql.types import StructType
from pyspark.sql.types import *
from pyspark.sql import SparkSession

#try changing local[2] -> url of lh
sc = SparkContext('local[2]','stream_test')
ssc = StreamingContext(sc, 1)
#sqlContext = SQLContext(sc)
spark=SparkSession.builder.appName('sparkdf').getOrCreate()

#add the frickin link

#We are currently getting json FILE as a DStream(check stream.py for JSON format)
#We need to extract the JSON and create an RDD -> List of tuples [(),(),..] where each Tuple contains (<Subject>,<Message>,<Spam/Ham>)	-----> REFER LINK FOR RDD FORMAT
#We need to pass the RDD to Dataframe creation

lines = ssc.socketTextStream("localhost", 6100)
words = lines.flatMap(lambda line: line.split(";"))

schema = StructType([StructField("Subject",StringType(),True),StructField("Message",StringType(),True),StructField("Spam/Ham",StringType(),True)])

def plswork(rdd):
	sdf=spark.createDataFrame(rdd,schema=schema)
	sdf.show()
#	df=rdd.toDF()
#	df.show(truncate=False)
#words.foreachRDD(lambda rdd: plswork(rdd))


def f(rdd):
	dataCol=rdd.collect()
	for row in dataCol:
		print(row)

words.foreachRDD(lambda rdd:f(rdd))




#sdf.show(n=2,truncate=False)
'''
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)
wordCounts.pprint()
'''
#sdf=sqlContext.createDataFrame(wordCounts)
#sdf.show(n=2,truncate=False)

ssc.start()

print('in between')
print(ssc.getActive())

ssc.awaitTermination()


ssc.stop(True,True)



print('whatever')


ssc.close()

''' 
    Define the input sources by creating input DStreams.
    Define the streaming computations by applying transformation and output operations to DStreams.
    Start receiving data and processing it using streamingContext.start().
    Wait for the processing to be stopped (manually or due to any error) using streamingContext.awaitTermination().
    The processing can be manually stopped using streamingContext.stop().
'''



'''

#ssc.awaitTermination()

'''
