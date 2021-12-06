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
	 
	plt.xlabel("x-axis")
	plt.ylabel("y-axis")
	plt.title("Bar chart")
	plt.show()
	
	
def multiple_line_plot(n,x,*y):
	count=0
	for i in y:
		count+=1
		plt.plot(x, i, label = "line "+"count")
		plt.legend()
		plt.show()
		
def multiple_bar_chart(x,y1,y2,y3,y4):
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
x=[]	
y=[]
fp=open('pac_100.txt')
lines=fp.readlines()
for i in lines:
	y.append(float(i))

for i in range(303):
	x.append(i+1)



#f, ax = plt.subplots(1)
#ax.set_ylim(ymin = 0.6)
plt.xlabel('Batch number')
plt.ylabel('Accuracy')
plt.title('SGD HyperParameter Tuning with Perceptron loss and constant Learning Rate for bsize=100')
plt.plot(x,y)
plt.show()
