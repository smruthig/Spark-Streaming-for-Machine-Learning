from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql.context import SQLContext
from pyspark.sql.types import StructType
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.functions import when
import sys
import numpy as np
import pickle

'''import nltk
nltk.download('wordnet')
nltk.download('stopwords')
nltk.download('punkt')'''

from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import HashingVectorizer
from nltk.tokenize import word_tokenize  #Other tokenizers are available. We are using this one for retrieving the words and punctuations.
from nltk.tokenize import RegexpTokenizer
from nltk.tokenize import word_tokenize, RegexpTokenizer
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import PorterStemmer
from nltk.stem import WordNetLemmatizer
from sklearn.metrics import accuracy_score, f1_score, confusion_matrix, precision_score, recall_score, roc_auc_score
from sklearn.naive_bayes import BernoulliNB
from sklearn import linear_model
from sklearn.cluster import MiniBatchKMeans
#import matplotlib.pyplot as plt


#FOR TEST
import pandas as pd


bsize = int(sys.argv[1])

sc = SparkContext('local[2]','stream_test')
ssc = StreamingContext(sc, 1)
#sqlContext = SQLContext(sc)
spark=SparkSession.builder.appName('sparkdf').getOrCreate()

lines = ssc.socketTextStream("localhost", 6100)

#MODELS
nb = BernoulliNB()
sgd = linear_model.SGDClassifier()
pac = linear_model.PassiveAggressiveClassifier()
clust = MiniBatchKMeans(n_clusters = 2)


#Metrics
pred_metrics=[]
y_true_metrics=[]
count = 0
f=open("Output_of_metrics.txt","a")


#Pickle
filename_nb = "nb.sav"
filename_sgd = "sgd.sav"
filename_pac = "pac.sav"
filename_kmeans = "kmeans.sav"


#x=0
def jsonToDf(rdd):
	#global x
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

	return(subject, message, spamham)


def preproc_pipeline(col):

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
		vector_sub = vectorizer_sub.transform(col)
		#enc=vector_sub.toarray()
		return (vocab, vector_sub)

	#HASHING VECTORIZER
	def hash_vec(col):
		vectorizer = HashingVectorizer(n_features=2**8)
		X = vectorizer.fit_transform(col)
		return X


	#PIPELINE OF OUR CHOICE
	preproc_1 = stemming(lemmatization(lowercase(tokenization_punctuation(col))))
	cv=[]
	#CONVERTING TO LIST OF STRINGS FROM LIST OFLIST OF WORDS(FOR CV)
	for i in preproc_1:
		cv.append(' '.join(i))

	#return cv
	return hash_vec(cv)


def naive_bayes(X, y, test_X, test_y):
	nb.partial_fit(X,y,classes=np.unique(y))

	pred=nb.predict(test_X)
	
	#ACCURACY
	print("SCORE", nb.score(test_X,test_y))
	
	#METRICS
	metrics_score(test_y,pred)
	
	#Pickle
	pickle.dump(nb, open(filename_nb, 'wb'))

def sgd_class(X, y, test_X, test_y):
	sgd.partial_fit(X,y,classes=np.unique(y))
	
	pred=sgd.predict(test_X)
	
	#ACCURACY
	print("SCORE", sgd.score(test_X,test_y))
	
	#METRICS
	metrics_score(test_y,pred)
	
	#Pickle
	pickle.dump(sgd, open(filename_sgd, 'wb'))

def pass_agg(X, y, test_X, test_y):
	pac.partial_fit(X,y,classes=np.unique(y))
	
	pred = pac.predict(test_X)
	
	#ACCURACY
	print("SCORE", pac.score(test_X,test_y))
	
	#METRICS
	metrics_score(test_y,pred)
	
	#Pickle
	pickle.dump(pac, open(filename_pac, 'wb'))


def clusters(X,y,test_X,test_y):
	clust.partial_fit(X,y)
	
	pred = clust.predict(test_X)

	#ACCURACY
	print("SCORE", nb.score(test_X,test_y))
	
	#METRICS
	metrics_score(test_y,pred)
	
	#Pickle
	pickle.dump(clust, open(filename_kmeans, 'wb'))


def parentFn(rdd, sub_test, msg_test, spamham_test):
	batch_df = jsonToDf(rdd)
	if batch_df:
		subject, message, spamham = preproc(batch_df)
		preproc_pipeline(subject)

		#COUNT VEC
		#vocab_train, preproc_sub_train = preproc_pipeline(subject)
		#vocab_test, preproc_sub_test = preproc_pipeline(sub_test)

		#HASH VEC
		preproc_sub_train = preproc_pipeline(subject)
		preproc_sub_test = preproc_pipeline(sub_test)

		naive_bayes(preproc_sub_train, spamham, preproc_sub_test, spamham_test)
		#pred_metrics=naive_bayes(preproc_sub_train, spamham, preproc_sub_test, spamham_test)
		#sgd_class(preproc_sub_train, spamham, preproc_sub_test, spamham_test)
		#pass_agg(preproc_sub_train, spamham, preproc_sub_test, spamham_test)
		#y_true_metrics=spamham_test
		#clusters(preproc_sub_train, spamham, preproc_sub_test, spamham_test)



def test_fn():
	test_data = pd.read_csv('/home/pes1ug19cs192/Desktop/BDProj/test.csv')
	mySchema = StructType([ StructField("Subject",StringType(), True),StructField("Message", StringType(), True),StructField("Spam/Ham", StringType(), True)])
	batch_test_df = spark.createDataFrame(test_data,schema=mySchema)
	batch_test_df = batch_test_df.withColumnRenamed("Spam/Ham","SpamHam")
	batch_test_df = batch_test_df.withColumn("SpamHam", when(batch_test_df.SpamHam == "Spam","1").when(batch_test_df.SpamHam == "Ham","0").otherwise(batch_test_df.SpamHam))
	pdtest=batch_test_df.toPandas()
	subject_test=pdtest['Subject']
	message_test=pdtest['Message']
	spamham_test=pdtest['SpamHam']
	return (subject_test, message_test, spamham_test)


def metrics_score(y_true, y_pred):
	accuracy = accuracy_score(y_true, y_pred)   #accuracy
	precision = precision_score(y_true,y_pred,pos_label='ham', average='binary')  		#precision
	f1 = f1_score(y_true,y_pred,pos_label='ham',average='binary')						#F1 score
	conf_matrix = confusion_matrix(y_true,y_pred)										#confusion matrix
	recall = recall_score(y_true,y_pred, pos_label='ham',average='binary')				#recall
	#roc_auc = roc_auc_score(y_true,y_pred)												#ROC-AUC curve
	
	print("ACCURACY",accuracy)
	print("PRECISION",precision)
	print("F1 SCORE",f1)
	print("CONF_MATRIX",conf_matrix)
	print("RECALL",recall)



sub_test, msg_test, spamham_test = test_fn()

lines.foreachRDD(lambda rdd: parentFn(rdd, sub_test, msg_test, spamham_test))

ssc.start()

print(ssc.getActive())
print("before call",pred_metrics)
#metrics_score(y_true_metrics, pred_metrics)
ssc.awaitTermination()

ssc.stop(True,True)

ssc.close()
