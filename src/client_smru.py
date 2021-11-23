from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql.context import SQLContext
from pyspark.sql.types import StructType
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.functions import when
import sys
from sklearn.feature_extraction.text import CountVectorizer
bsize = int(sys.argv[1])

sc = SparkContext('local[2]','stream_test')
ssc = StreamingContext(sc, 1)
#sqlContext = SQLContext(sc)
spark=SparkSession.builder.appName('sparkdf').getOrCreate()

lines = ssc.socketTextStream("localhost", 6100)
x=0
def parseJson(rdd):
	global x
	global bsize	
	if not rdd.isEmpty():
		print("batch no: ",x)
		x+=1
		df = spark.read.json(rdd)
		#df.printSchema()
		
		#df.createOrReplaceTempView("mytable")
		#df2 = spark.sql("SHOW COLUMNS FROM mytable")
		#df2.show()
		
		# So it is creating a table with col name as row no. in batch, and data as a json of the entire row
		# eg: if 5 records passed per batch,
		# col names: 	0	1	2	3	4
		# data:	row1	row2	row3	row4	row5
		# each row is a struct which further splits into the 3 features
		
		# TO FLATTEN (didn't work):
		#json_parsed = spark.read.json(df.rdd.map(lambda row: row.0))
		#json_parsed.printSchema()
		
		# TO FLATTEN (worked)
		batch_df = df.select(col("0.*")).withColumnRenamed('feature0','Subject').withColumnRenamed('feature1','Message').withColumnRenamed('feature2','Spam/Ham')

		for row_no in range(1, bsize):
			s = str(row_no)+".*"
			df3 = df.select(col(s)).withColumnRenamed('feature0','Subject').withColumnRenamed('feature1','Message').withColumnRenamed('feature2','Spam/Ham')
			batch_df = batch_df.union(df3)
			#duplicates removed
			#batch_df.show(truncate=True)
			#print(batch_df.count())
		#batch_df.groupBy("Spam/Ham").count().orderBy(col("count").desc()).show()
		#msg_doc = DocumentAssembler().setInputCol("Message").setOutputCol("msg_doc")
		
		#rdd2=batch_df.rdd.map(lambda x[2]: x[2]=='Spam'?1:0)
		#dfn=rdd2.toDF(["Subject","Message","Spam/Ham"]   )
		#dfn.show()
		
		#batch_df.update("Spam/Ham = 'Spam'", { "Spam/Ham": "1" } )
		#batch_df.update("Spam/Ham = 'Ham'", { "Spam/Ham": "0" } )
		#batch_df.show()
		

		#dfl = batch_df.withColumn("Spam/Ham", when(df.Subject == "Spam","1").when(df.Spam/Ham == "Ham","0").otherwise(df.gender))
		#dfl.show()
		
		pdtrain = batch_df.toPandas()
		#trainseries = pdtrain['Subject'].apply(lambda x : np.array(x.toArray())).as_matrix().reshape(-1,1)
		#print(pdtrain)
		#print(pdtrain['Subject'])
		#print(document)
		
		subject=pdtrain['Subject']
		vectorizer = CountVectorizer()
		vectorizer.fit(subject)
		print("Vocabulary: ", vectorizer.vocabulary_)
		vector = vectorizer.transform(subject)
		print("Encoded Document is:")
		print(vector.toarray())
		
		message=pdtrain['Message']	
		vectorizer1 = CountVectorizer()
		vectorizer1.fit(message)
		print("Vocabulary: ", vectorizer.vocabulary_)
		vector1 = vectorizer.transform(message)
		print("Encoded Document is:")
		print(vector1.toarray())
		
		
		df1 = batch_df.withColumnRenamed("Spam/Ham","SpamHam")
		dfl = df1.withColumn("SpamHam", when(df1.SpamHam == "spam","1").when(df1.SpamHam == "ham","0").otherwise(df1.SpamHam))
		dfl.show()


lines.foreachRDD(lambda rdd: parseJson(rdd))

ssc.start()

print(ssc.getActive())

ssc.awaitTermination()

ssc.stop(True,True)

ssc.close()
