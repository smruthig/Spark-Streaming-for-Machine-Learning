import sys
import numpy as np
from sklearn.metrics import accuracy_score, f1_score, confusion_matrix, precision_score, recall_score, roc_auc_score


def metrics_score(y_true, y_pred):
	accuracy = accuracy_score(y_true, y_pred)   #accuracy
	precision = precision_score(y_true,y_pred,pos_label='spam', average='binary')  		#precision
	f1 = f1_score(y_true,y_pred,pos_label='spam',average='binary')				#F1 score
	conf_matrix = confusion_matrix(y_true,y_pred)							#confusion matrix
	recall = recall_score(y_true,y_pred, pos_label='spam',average='binary')			#recall
	#roc_auc = roc_auc_score(y_true,y_pred)							#ROC-AUC curve
	#print(conf_matrix,accuracy,precision,f1,recall)
  print("confusion matrix :",conf_matrix)
  print("accuracy :",accuracy)
  print("precision :",precision)
  print("f1 :", f1)
  print("recall :", recall)
