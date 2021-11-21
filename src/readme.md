1. Client_df_trials:  
Trial files to convert the received data into dataframe of correct format  
  
2. Client_streamConn_trials:  
Trial files to get client to connect to the port and receive data  
  
3. Server_stream - includes final files:  
Modifications to server stream.py   
  
4. cc1+df_v1.2_final:  
(cc1)=>client_conn_final_v1 used for connecting client to port to receive data   
(df)=>Converts to df of reqd format  
(v1.2)=>builds upon v1 in Client_df_trials  
(final)=>working code  
To be used with stream_final.py(for server side)  

5. client_conn_final_v1:  
One working way of receiving data from server.  
Does not terminate, throws error.  

6. client_conn_final_v2:  
Another working way of receiving data from server.  
Does not terminate, but waits for user to terminate. Does not throw error.  

7. NLPtest

  
