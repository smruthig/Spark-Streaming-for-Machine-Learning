#from https://nlp.johnsnowlabs.com/docs/en/concepts

import sparknlp
sparknlp.start()

from sparknlp.pretrained import PretrainedPipeline

explain_document_pipeline = PretrainedPipeline("explain_document_ml")
#pass a single row from the dataframe we create as parameter for annotate?
annotations = explain_document_pipeline.annotate("We are very happy about SparkNLP")
print(annotations)

'''OUTPUT:
{
  'stem': ['we', 'ar', 'veri', 'happi', 'about', 'sparknlp'],
  'checked': ['We', 'are', 'very', 'happy', 'about', 'SparkNLP'],
  'lemma': ['We', 'be', 'very', 'happy', 'about', 'SparkNLP'],
  'document': ['We are very happy about SparkNLP'],
  'pos': ['PRP', 'VBP', 'RB', 'JJ', 'IN', 'NNP'],
  'token': ['We', 'are', 'very', 'happy', 'about', 'SparkNLP'],
  'sentence': ['We are very happy about SparkNLP']
}'''

_____________________________________________________________________________________

#to use pretrained model on dataframe

sentences = [
  ['Hello, this is an example sentence'],
  ['And this is a second sentence.']
]

# spark is the Spark Session automatically started by pyspark.
data = spark.createDataFrame(sentences).toDF("text")

# Download the pretrained pipeline from Johnsnowlab's servers
explain_document_pipeline = PretrainedPipeline("explain_document_ml")

'''OUTPUT:
explain_document_ml download started this may take some time.
Approx size to download 9.4 MB
[OK!]'''

# Transform 'data' and store output in a new 'annotations_df' dataframe
annotations_df = explain_document_pipeline.transform(data)

# Show the results
annotations_df.show()

'''OUTPUT:
+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+
|                text|            document|            sentence|               token|             checked|               lemma|                stem|                 pos|
+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+
|Hello, this is an...|[[document, 0, 33...|[[document, 0, 33...|[[token, 0, 4, He...|[[token, 0, 4, He...|[[token, 0, 4, He...|[[token, 0, 4, he...|[[pos, 0, 4, UH, ...|
|And this is a sec...|[[document, 0, 29...|[[document, 0, 29...|[[token, 0, 2, An...|[[token, 0, 2, An...|[[token, 0, 2, An...|[[token, 0, 2, an...|[[pos, 0, 2, CC, ...|'''
