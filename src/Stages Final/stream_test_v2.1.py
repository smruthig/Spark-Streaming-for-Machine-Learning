from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc=SparkContext('local[2]','bdproj')
ssc=StreamingContext(sc,1)
lines=ssc.socketTextStream('127.0.0.1',6100)

ssc.start()
print(lines)

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()


# Read text from socket
socketDF = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 6100) \
    .load()

print(socketDF.isStreaming)   # Returns True for DataFrames that have streaming sources

print(socketDF)

ssc.awaitTermination()
#printSchema()
# Read all the csv files written atomically in a directory
'''

userSchema = StructType().add("Subject", "string").add("Message", "string").add("Spam/Ham", "string")
csvDF = spark \
    .readStream \
    .option("sep", ";") \
    .schema(userSchema) \
    .csv("/path/to/directory")  # Equivalent to format("csv").load("/path/to/directory")
    '''
