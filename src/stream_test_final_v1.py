from pyspark import SparkContext
from pyspark.streaming import StreamingContext

#try changing local[2] -> url of lh
sc = SparkContext('local[2]','stream_test')
ssc = StreamingContext(sc, 1)

lines = ssc.socketTextStream("localhost", 6100)
words = lines.flatMap(lambda line: line.split(";"))

pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

# Print the first ten elements of each RDD generated in this DStream to the console
wordCounts.pprint()

ssc.start()

print('in between')
print(ssc.getActive())

ssc.awaitTermination()

print('whatever')
''' 
    Define the input sources by creating input DStreams.
    Define the streaming computations by applying transformation and output operations to DStreams.
    Start receiving data and processing it using streamingContext.start().
    Wait for the processing to be stopped (manually or due to any error) using streamingContext.awaitTermination().
    The processing can be manually stopped using streamingContext.stop().
'''
