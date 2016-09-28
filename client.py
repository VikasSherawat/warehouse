import os
import time
from transactions import Transactions

def readFile(fname):
	with open(fname) as f:
		content = [x.strip('\n') for x in f.readlines()]
	t = Transactions()
	i = 0
	tcount = 0
	while i < len(content):
		transx = content[i].split(",");
		transType = transx[0]
		tcount =  tcount + 1
		if transType == 'N':
			#print "New Order type transaction"
			M = int(transx[4])
			c = int(transx[1])
			w = int(transx[2])
			d = int(transx[3])
			lis = content[i+1:i+M]
			t.neworder(c,w,d,lis)
			i = i + M
		elif transType == 'D':
			print "Delivery type transaction"
		elif transType == 'P':
			print "Payment type transaction"
		elif transType == 'S':
			w = int(transx[1])
			d = int(transx[2])
			th  = int(transx[3])
			l = int(transx[4])
			t.stocklevel(w,d,th,l)
		elif transType == 'O':
			print "Order status transaction"
		elif transType == 'I':
			w = int(transx[1])
			d = int(transx[2])
			l = int(transx[3])
			t.popularItem(w,d,l)
		else:
			t.topbalance()
		i = i+1;
	return tcount;

def readTransactions(start, end):
	#print 'Inside readTransactions'
	count = 0;
	for i in range(start,end):
		filepath = os.getcwd()+'/D-xact-revised/'+str(i)+'.txt'
		#print filepath
		count = count + readFile(filepath)
	return count;


if __name__ == "__main__":
	#print 'Inside main'
	start = time.time()
	count =	readTransactions(0,1)
	print "Total Transactions processed are:",count
	rtime = time.time()-start
	throughput =rtime/count 		
	print "Total Time taken in seconds:",rtime
	print "Throughput =",throughput
