<p align="center">
    <h1 align="center">Spark Streaming for Machine Learning</h1>
</p>
<p align="center">
		<em>Developed with the software and tools below.</em>
</p>
<p align="center">
	<img src="https://img.shields.io/badge/NLTK-FF6F00.svg?style=flat&logo=NLTK&logoColor=white" alt="NLTK">
	<img src="https://img.shields.io/badge/scikitlearn-F7931E.svg?style=flat&logo=scikit-learn&logoColor=white" alt="scikitlearn">
	<img src="https://img.shields.io/badge/Jupyter-F37626.svg?style=flat&logo=Jupyter&logoColor=white" alt="Jupyter">
	<img src="https://img.shields.io/badge/PySpark-D00000.svg?style=flat&logo=PySpark&logoColor=white" alt="PySpark">
	<img src="https://img.shields.io/badge/matplotlib-8CAAE6.svg?style=flat&logo=matplotlib&logoColor=white" alt="matplotlib">
	<br>
	<img src="https://img.shields.io/badge/Python-3776AB.svg?style=flat&logo=Python&logoColor=white" alt="Python">
	<img src="https://img.shields.io/badge/pandas-150458.svg?style=flat&logo=pandas&logoColor=white" alt="pandas">
	<img src="https://img.shields.io/badge/NumPy-013243.svg?style=flat&logo=NumPy&logoColor=white" alt="NumPy">
</p>
<hr>

## Quick Links

- [Overview](#overview)
- [Instructions to Run the Streaming](#instructions-to-run-the-streaming)
- [Design Details](#design-details)
  - [Streaming Data and Preprocessing](#streaming-data-and-preprocessing)
  - [Modelling and Incremental Learning](#modelling-and-incremental-learning)
  - [Model Algorithms](#model-algorithms)
  - [Surface Level Implementation Details](#surface-level-implementation-details-about-each-unit)
  - [Reason Behind Design Decisions](#reason-behind-design-decisions)
 
## Overview

This project is aimed at trying to classify emails to spam and ham categories using ML algorithms on Big Data.  
The dataset used for training is provided in the folder "data".  
The link for the original dataset is provided here:  
https://www.kaggle.com/wanderfj/enron-spam  

## Instructions to Run the Streaming
The data is being streamed and read on-the-go from port 6100 on localhost using TCP connection via Spark Streaming.  
1. Open a terminal in the `src` folder.
2. Run `python3 stream.py -f <path>/test.csv -b <batch_size>`
3. Open another terminal in `~/spark-3.1.2-bin-hadoop3.2/sbin`
4. Run `/opt/spark/bin/spark-submit '<path>/stream_test_final_v1.py' localhost 6100`

  
##  Design Details
	
### Streaming Data and Preprocessing

In our project, we have designed the system to take the streaming dataset and its batch size as command-line arguments. Upon receiving the RDD through the TCP socket (using port 6100), each batch is converted into a dataframe before any further actions are taken.

For convenience and standardization, we transform all 'Spam' entries to 1 and 'Ham' entries to 0 in the Spam/Ham column. This ensures uniformity in the target labels across the dataset.

Once the dataframe is prepared, we convert it into a pandas-like dataframe to facilitate easy feature extraction and manipulation. The preprocessing pipeline involves standard NLP techniques implemented using Python's NLTK library, including tokenization, lemmatization, and stemming. Additionally, we leverage the HashVectorizer function from the sklearn library to obtain frequencies of words, converting text data into a numerical representation.

### Modelling and Incremental Learning

Our modelling approach relies on the sklearn library, where we implement various machine learning algorithms suitable for incremental learning. The incremental learning process is facilitated through the use of the `partial_fit()` function available in sklearn. This function allows us to update the model iteratively with each batch of data, ensuring that the model adapts to changes in the streaming data over time.

By combining streaming data processing, preprocessing techniques, and incremental learning, our system is capable of efficiently processing and analyzing large volumes of streaming data while continuously updating and improving its classification models.

### Model Algorithms

In this project, we have implemented three machine learning algorithms suitable for incremental learning and binary classification tasks. Each of these algorithms has been chosen for its unique advantages in handling streaming data and its effectiveness in text classification problems such as spam detection.

- **Naive Bayes (Bernoulli):** 
  - This is a variant of Naive Bayes designed specifically for binary/boolean features. It is highly effective for spam detection due to its assumption of independence among predictors. Bernoulli Naive Bayes is particularly suitable for text classification where the presence or absence of a feature (word) contributes to the spam or ham classification.
  
- **Stochastic Gradient Descent (SGD):**
  - SGD is a simple yet very efficient approach to fitting linear classifiers and regressors under convex loss functions such as linear SVM and logistic regression. Unlike standard gradient descent, which processes all the training data at once, SGD updates the parameters incrementally for each training example. This makes it particularly useful for large-scale and streaming data applications.
  
- **Passive Aggressive Classifier:**
  - The Passive Aggressive Classifier is a type of online learning algorithm that remains passive for a correct classification outcome and becomes aggressive in the event of a miscalculation, adjusting the model significantly. It

All these algorithms are available in scikit-learn and support incremental learning.

### Surface level implementation details about each unit:

- **Streaming the data**:
  - Utilizes the `stream.py` file provided to stream the train and test datasets for the respective programs via a TCP socket. The batch size is taken as a command line argument.

- **Preprocessing Techniques:**
  1. **Converting target column to binary:** 
     - Spam and Ham are converted to 0 and 1.
  2. **Tokenization and Punctuation:** 
     - Tokenization splits sentences into tokens of words, and punctuation removal eliminates punctuations from the text.
  3. **Lowercase conversion:** 
     - Converts the text into lowercase.
  4. **Lemmatization:** 
     - Removes the ending part of words and retains only the base word.
  5. **Stemming:** 
     - Similar to lemmatization for retaining the morphological base words.
  6. **Hash vectorizer:** 
     - Used to map every word to a numerical value as a vector/array.

- **Incremental Learning:**
  1. **partial_fit():** 
     - Incrementally fits on batches of samples.
     - Called consecutively for different chunks of the dataset to learn incrementally.
     - Updates the model with the incremental data.
  2. **Pickle files:** 
     - Store and retrieve updated models.

- **Modelling:**
  - For each model (Bernoulli NB, SGD, and PAC):
    - Uses `partial_fit()` to fit each batch incrementally.
    - `predict()` is applied on the test data to obtain the predicted Spam/Ham column.
    - `score()` is used to assess the accuracy of the predicted values against the true values.

- **Clustering:**
  - Utilizes MiniBatchKMeans for clustering.
  - Number of clusters is set to 2, corresponding to the class labels (Spam-1 and Ham-0).

### Reason behind design decisions:

- **Preprocessing Pipeline:** 
  - Employs standard NLP preprocessing techniques using NLTK to facilitate predictions and improve model accuracy.
  - Hash vectorizer is used to convert text to numerical form and limit the number of features.
  
- **Incremental Learning:** 
  - Implemented using scikit-learn for its ease-of-use and built-in classification modelling functions.
  - `partial_fit()` ensures incremental learning with each batch.

- **Model Choices:**
  - Naive Bayes: 
    - Popularly used for text classification due to its simplicity and decreased computational complexity.
  - SGD: 
    - Beneficial in minimizing the loss function.
  - PAC: 
    - Suitable for large-scale learning, thus chosen for implementation.

- **Clustering:** 
  - MiniBatchKMeans facilitates incremental clustering.
