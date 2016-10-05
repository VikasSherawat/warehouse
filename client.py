import os
import sys
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
		print "------------------------------------------------------------------------------------------------------------------------------------"  
		if transType == 'N':
			print "New Order type transaction======================================================================================="
			M = int(transx[4])
			c = int(transx[1])
			w = int(transx[2])
			d = int(transx[3])
			lis = content[i+1:i+M+1]
			t.neworder(c,w,d,lis)
			i = i + M
		elif transType == 'D':
			print "Delivery type transaction======================================================================================="
			w = int(transx[1])
			c = int(transx[2])
			t.delivery(w,c)
		elif transType == 'P':
			print "Payment type transaction========================================================================================="
			w = int(transx[1])
			d = int(transx[2])
			c = int(transx[3])
			p = int(transx[4])
			t.payment(c,w,d,p)
		elif transType == 'S':
			print "Stock level transaction=========================================================================================="
			w = int(transx[1])
			d = int(transx[2])
			th  = int(transx[3])
			l = int(transx[4])
			t.stocklevel(w,d,th,l)
		elif transType == 'O':
			print "Order status transaction======================================================================================="
			w = int(transx[1])
			d = int(transx[2])
			c = int(transx[3])
			t.orderstatus(w,d,c)
		elif transType == 'I':
			print "Popular Item transaction======================================================================================="
			w = int(transx[1])
			d = int(transx[2])
			l = int(transx[3])
			t.popularItem(w,d,l)
		elif transType == 'T':
			print "Top Balance transaction========================================================================================="
			t.topbalance()
		else:
			print 'Input Mistmatch'
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
	begin = int(sys.argv[1])
	end = int(sys.argv[2])
	count =	readTransactions(begin,end+1)
	print "Total Transactions processed are:",count
	rtime = time.time()-start
	throughput =rtime/count 		
	print "Total Time taken in seconds:",rtime
	print "Throughput =",throughput
