import matplotlib.pyplot as plt
def line_plot(x,y):
	plt.plot(x,y)
	plt.xlabel('x - axis')
	plt.ylabel('y - axis')
	plt.title('Line graph')
	plt.show()
	
def bar_chart(x,y):

	fig = plt.figure(figsize = (10, 5))
	 
	# creating the bar plot
	plt.bar(x,y, color ='maroon',width = 0.4)
	 
	plt.xlabel("Models")
	plt.ylabel("Accuracy")
	plt.title("Bar chart for different models ")
	plt.show()
	
	
def multiple_line_plot(x,*y):
	count=0
	for i in y:
		count+=1
		plt.plot(x, i, label = "line "+"count")
		plt.legend()
		plt.show()
		
def multiple_bar_chart(x,y1,y2,y3):
		width=0.2
		plt.bar(x-0.2, y1, width, color='cyan')
		plt.bar(x, y2, width, color='orange')
		plt.bar(x+0.2, y3, width, color='green')
		plt.bar(x+0.2, y4, width, color='blue')
		plt.xticks(x, ['Naive Bayes', 'SGD', 'PAC'])
		plt.xlabel("Models")
		plt.ylabel("Metrics")
		plt.legend(["Accuracy","Precision","Recall","F1-score"])
		plt.show()
		
y1=[0.86,0.78,0.96,0.95]
y2=[0.96,0.88,0.90,0.91]
y3=[0.90,0.88,0.90,0.86]
x=['NB','SGD','PAC']

#multiple_bar_chart(x,y1,y2,y3)


def plott(pred_spam, pred_ham, true_spam, true_ham):
	x = ['Spam','Ham']
	X_axis = np.arange(len(x))

	Ygirls = [pred_spam,pred_ham]
	Zboys = [true_spam,true_ham]
	plt.bar(X_axis - 0.2, Ygirls, 0.4, label = 'Predicted', color = 'blue')
	plt.bar(X_axis + 0.2, Zboys, 0.4, label = 'True', color = 'green')
	plt.xticks(X_axis, x)
	plt.xlabel("Spam/Ham")
	plt.ylabel("True/Predicted")
	plt.title("Number of True/Predicted in each Spam/Ham")
	plt.legend()
	plt.show()

#plott(,,,)


	
'''
x=[]	
y1=[]
y2=[]
y3=[]
fp1=open('nb_50.txt')
fp2=open('sgd_50.txt')
fp3=open('pac_50.txt')
lines1=fp1.readlines()
lines2=fp2.readlines()
lines3=fp3.readlines()
for i in lines1:
	y1.append(float(i))

for i in lines2:
	y2.append(float(i))

for i in lines3:
	y3.append(float(i))


for i in range(606):
	x.append(i+1)
'''

#bar_chart(['NB','SGD','PAC'],[0.9569,0.9970,0.9964])
'''
plt.xlabel('Models')
plt.ylabel('Accuracy')
plt.plot(x,y1,label="NB")
plt.plot(x,y2,label="SGD")
plt.plot(x,y3,label="PAC")
plt.legend()
plt.show()
'''


'''
#f, ax = plt.subplots(1)
#ax.set_ylim(ymin = 0.6)
plt.xlabel('Batch number')
plt.ylabel('Accuracy')
plt.title('SGD HyperParameter Tuning with Perceptron loss and constant Learning Rate for bsize=100')
plt.plot(x,y)
plt.show()'''


#multiple_line_plot(x,)
x=[]
y=[]
fp=open('pac_ptuning.txt')
lines=fp.readlines()
for i in lines:
	y.append(float(i))
	
for i in range(303):
	x.append(i+1)

plt.xlabel('Batch number')
plt.ylabel('Accuracy')
plt.title('PAC for bsize=100 with Hyper Parameter Tuning')
plt.plot(x,y)
plt.show()

'''
x=[]	
y1=[]
y2=[]
y3=[]
fp1=open('nb_500.txt')
fp2=open('sgd_500.txt')
fp3=open('pac_500.txt')
lines1=fp1.readlines()
lines2=fp2.readlines()
lines3=fp3.readlines()
for i in lines1:
	y1.append(float(i))

for i in lines2:
	y2.append(float(i))

for i in lines3:
	y3.append(float(i))


for i in range(35):
	x.append(i+1)
	
plt.xlabel('Models')
plt.ylabel('Accuracy')
plt.plot(x,y1,label="NB")
plt.plot(x,y2,label="SGD")
plt.plot(x,y3,label="PAC")
plt.legend()
plt.show()
'''

'''
#bar_chart(['NB','SGD','PAC'],[0.9558,0.9967,0.9958])
x=[]
y50=[]
y100=[]
y500=[]
f50=open('pac_50.txt')
f100=open('pac_100.txt')
f500=open('pac_500.txt')
for i in f50.readlines():
	y50.append(float(i))
	
for i in f100.readlines():
	y100.append(float(i))
	
for i in f500.readlines():
	y500.append(float(i))
	
for i in range(35):
	x.append(i+1)

plt.xlabel('Batch number')
plt.ylabel('Accuracy')
plt.title('PAC for different batch sizes')
plt.plot(x,y50[:35],label='bsize=50')
plt.plot(x,y100[:35],label='bsize=100')
plt.plot(x,y500[:35],label='bsize=500')
plt.legend()
plt.show()
'''


