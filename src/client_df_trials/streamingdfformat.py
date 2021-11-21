'''In a lot of places, they only spoke about using either dataframes or text as input for Spark NLP functions.
If we use text, it might take too long bc we have SO many records. And also bc we need the Spam/Ham column for each message as well for our model.
So I think converting the input from localhost to a dataframe is our best bet.'''

#from https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html

# Create DataFrame representing the stream of input lines from connection to localhost:9999
lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Split the lines into words
words = lines.select(
   explode(
       split(lines.value," ")
   ).alias("word")
)

# Generate running word count
wordCounts = words.groupBy("word").count()

'''This lines DataFrame represents an unbounded table containing the streaming text data.
This table contains one column of strings named “value”, and each line in the streaming text data becomes a row in the table.
Note, that this is not currently receiving any data as we are just setting up the transformation, and have not yet started it.
Next, we have used two built-in SQL functions - split and explode, to split each line into multiple rows with a word each.
In addition, we use the function alias to name the new column as “word”.
Finally, we have defined the wordCounts DataFrame by grouping by the unique values in the Dataset and counting them.
Note that this is a streaming DataFrame which represents the running word counts of the stream.'''
