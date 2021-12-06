This folder contains pickle files and the output of model prediction and scoring on test data.

The files are named as follows-

  For pickle files (.sav): ModelName_ParametersTuned_TrainDataBatchSize.sav (IF NO HYPERPARAMETERS HAVE BEEN GIVEN, ParametersTuned is 'no')
  
  For output files (.txt): ModelName_ParametersTuned_TrainDataBatchSize_test.txt 
  
 Each output file contains specifications about the model, whether hyperparameters have been modified and which ones, batch size of the train data that generated the pickle file, and batch size of the test data.
 
 Test data batch size has been taken as a constant across all output files as 500.
