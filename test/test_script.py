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

bsize = int(sys.argv[1])
nb_path = sys.argv[2]
sgd_path = sys.argv[3]
pac_path = sys.argv[4]
clust_path = sys.argv[5]


sc = SparkContext('local[2]','stream_test')
ssc = StreamingContext(sc, 1)
#sqlContext = SQLContext(sc)
spark=SparkSession.builder.appName('sparkdf').getOrCreate()

lines = ssc.socketTextStream("localhost", 6100)

nb = pickle.load(open(nb_path, 'rb'))
sgd = pickle.load(open(sgd_path, 'rb'))
pac = pickle.load(open(pac_path, 'rb'))
clust = pickle.load(open(clust_path, 'rb'))

def jsonToDf(rdd):
	global x
	global bsize
	if not rdd.isEmpty():

		df = spark.read.json(rdd)

		# TO FLATTEN (worked)
		batch_df = df.select(col("0.*")).withColumnRenamed('feature0','Subject').withColumnRenamed('feature1','Message').withColumnRenamed('feature2','Spam/Ham')

		for row_no in range(1, bsize):
			s = str(row_no)+".*"
			df3 = df.select(col(s)).withColumnRenamed('feature0','Subject').withColumnRenamed('feature1','Message').withColumnRenamed('feature2','Spam/Ham')
			batch_df = batch_df.union(df3)

		return batch_df
	return None

def preproc(batch_df):

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

def metrics_score(y_true, y_pred):
	accuracy = accuracy_score(y_true, y_pred)   #accuracy
	precision = precision_score(y_true,y_pred,pos_label='spam', average='binary')  		#precision
	f1 = f1_score(y_true,y_pred,pos_label='spam',average='binary')				#F1 score
	conf_matrix = confusion_matrix(y_true,y_pred)							#confusion matrix
	recall = recall_score(y_true,y_pred, pos_label='spam',average='binary')			#recall
	#roc_auc = roc_auc_score(y_true,y_pred)							#ROC-AUC curve
	#print(conf_matrix,accuracy,precision,f1,recall)
  	print("Confusion matrix :",conf_matrix)
 	print("Accuracy :",accuracy)
  	print("Precision :",precision)
  	print("F1 :", f1)
  	print("Recall :", recall)


def parentFn(rdd):
	batch_df = jsonToDf(rdd)
	if batch_df:
		subject, message, spamham = preproc(batch_df)
		#preproc_pipeline(subject)

		#COUNT VEC
		#vocab_train, preproc_sub_train = preproc_pipeline(subject)
		#vocab_test, preproc_sub_test = preproc_pipeline(sub_test)

		#HASH VEC
		#preproc_sub_train = preproc_pipeline(subject)
		preproc_sub_test = preproc_pipeline(subject)

		#naive_bayes(preproc_sub_train, spamham, preproc_sub_test, spamham_test)
		#pred_metrics=naive_bayes(preproc_sub_train, spamham, preproc_sub_test, spamham_test)
		#sgd_class(preproc_sub_train, spamham, preproc_sub_test, spamham_test)
		#pass_agg(preproc_sub_train, spamham, preproc_sub_test, spamham_test)
		##y_true_metrics=spamham_test
		#clusters(preproc_sub_train, spamham, preproc_sub_test, spamham_test)

        print("**************NAIVE-BAYES*****************\n")
        pred_nb = nb.predict(preproc_sub_test)
	    print("Scores for each batch\n")
	    print(nb.score(preproc_sub_test,spamham))
        print("Metrics for Naive-Bayes\n")
        metrics_score(spamham,pred_nb)

        print("**************SGD*****************\n")
        pred_sgd = sgd.predict(preproc_sub_test)
	    print("Scores for each batch\n")
	    print(sgd.score(preproc_sub_test,spamham))
        print("Metrics for SGD\n")
        metrics_score(spamham,pred_sgd)


        print("**************PAC*****************\n")
        pred_pac = pac.predict(preproc_sub_test)
	    print("Scores for each batch\n")
	    print(pac.score(preproc_sub_test,spamham))
        print("Metrics for PAC\n")
        metrics_score(spamham,pred_pac)

        print("**************CLUSTERING*****************\n")
        pred_clust = clust.predict(preproc_sub_test)
	    print("Scores for each batch\n")
	    print(clust.score(preproc_sub_test,spamham))
        print("Matrics for Clustering\n")
        metrics_score(spamham,pred_clust)


lines.foreachRDD(lambda rdd: parentFn(rdd))

ssc.start()

print(ssc.getActive())
print("before call")
#metrics_score(y_true_metrics, pred_metrics)
ssc.awaitTermination()

ssc.stop(True,True)

ssc.close()
