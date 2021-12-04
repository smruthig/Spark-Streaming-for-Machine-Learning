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
from nltk.tokenize import word_tokenize  #Other tokenizers are available. We are using this one for retrieving the words and punctuations.
from nltk.tokenize import RegexpTokenizer
from nltk.tokenize import word_tokenize, RegexpTokenizer
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import PorterStemmer
from nltk.stem import WordNetLemmatizer



bsize = int(sys.argv[1])

sc = SparkContext('local[2]','stream_test')
ssc = StreamingContext(sc, 1)
#sqlContext = SQLContext(sc)
spark=SparkSession.builder.appName('sparkdf').getOrCreate()

lines = ssc.socketTextStream("localhost", 6100)

#x=0
def jsonToDf(rdd):
	global x
	global bsize
	if not rdd.isEmpty():
		#print("batch no: ",x)
		#x+=1
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
		return batch_df
	return None

def preproc(batch_df):
	# ENCODING Spam/ham col(didn't work):
	#batch_df.update("Spam/Ham = 'Spam'", { "Spam/Ham": "1" } )
	#batch_df.update("Spam/Ham = 'Ham'", { "Spam/Ham": "0" } )
	#batch_df.show()

	#ENCODING Spam/ham col(worked):
	batch_df = batch_df.withColumnRenamed("Spam/Ham","SpamHam")
	batch_df = batch_df.withColumn("SpamHam", when(batch_df.SpamHam == "Spam","1").when(batch_df.SpamHam == "Ham","0").otherwise(batch_df.SpamHam))
	#batch_df.show()

	#Converting to pandas-like df:
	pdtrain = batch_df.toPandas()

	subject=pdtrain['Subject']
	message=pdtrain['Message']
	spamham=pdtrain['SpamHam']
    
    #TOKENIZER and PUNCTUATION REMOVER
    def tokenization_punctuation(col):
        tzer = RegexpTokenizer(r"\w+")
        tok = [tzer.tokenize(line) for line in col]
        return tok

    #LOWERCASE
    def lowercase(col):
        low = []  
        for i in col:
            inner_low = []
            for j in i:
                inner_low.append(j.lower())
            low.append(inner_low)
        return low
	
    #LEMMATIZATION
    def lemmatization(col):
        lemmatizer = WordNetLemmatizer()
        lem=[]
        for text in col:
            inner_lem=[]
            for word in text:
                inner_lem.append(lemmatizer.lemmatize(word))
            lem.append(inner_lem)
        return lem
			
			
	#STEMMING
    def stemming(col):
        s = []
        ps = PorterStemmer()
        for text in col:	
            inner_s=[]
            for word in text:
	    		inner_s.append(ps.stem(word))
            s.append(inner_s)
        return s
        
	#COUNT VECTORIZER
    def count_vec(col):
        vectorizer_sub = CountVectorizer()
        vectorizer_sub.fit(col)
        vocab=vectorizer_sub.vocabulary_
        vector_sub = vectorizer_sub.transform(subject)
        enc=vector_sub.toarray()
        return (vocab, enc)
    
    return count_vec(stemming(lemmatization(lowercase(tokenization_punctuation(subject)))))
    

def parentFn(rdd):
	batch_df = jsonToDf(rdd)
	if batch_df:
		prep = preproc(batch_df)
        print(prep)

lines.foreachRDD(lambda rdd: parentFn(rdd))

ssc.start()

print(ssc.getActive())

ssc.awaitTermination()

ssc.stop(True,True)

ssc.close()
