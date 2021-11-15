# Spark Streaming for Machine Learning

This project is aimed at trying to classify emails to spam and ham categories using ML algorithms on Big Data.  
The dataset used for training is provided in the folder "data".  
The link for the original dataset is provided here:  
https://www.kaggle.com/wanderfj/enron-spam  

The data is being streamed and read on-the-go from port 6100 on localhost using TCP connection via Spark Streaming.  
The instructions to run the streaming are:  
1. Open a terminal in src folder.  
2. Run python3 stream.py -f <path>/test.csv -b <batch_size>
3. Open another terminal in ~/spark-3.1.2-bin-hadoop3.2/sbin
4. Run /opt/spark/bin/spark-submit '<path>/stream_test_final_v1.py' localhost 6100
