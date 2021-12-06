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
