from __future__ import division
import os
import sys
import time
from transactions import Transactions
import threading
import time
import traceback

class MyThread (threading.Thread):

        def __init__(self, id):
                threading.Thread.__init__(self)
                self.id = id

        def run(self):
		start = time.time()
		count = 0
		try:
			fname = os.getcwd()+'/trans/'+str(self.id)+'.txt'
                	count = self.readFile(fname)
		except Exception as e:
			print "Thread:"+str(self.id)+" interrupted"
			traceback.print_exc()
		finally:
			rtime = time.time()-start
			throughput =count/rtime
			out = "File finished:"+str(self.id)+","+str(count)+","+str(rtime)+","+str(throughput)
			print out
			MainThread.totalcount += count
			MainThread.totaltime += rtime
			MainThread.filesfinish +=1
			ls = []
			ls.append(throughput)
			if MainThread.filesfinish == MainThread.totalfiles:
				timetaken = time.time()-MainThread.starttime
				print "Total Transaction processed:"+str(MainThread.totalcount)
				print "Total Time taken:"+str(timetaken)
				print "Total Throughput:"+str(MainThread.totalcount/timetaken)
				print "Maximum Throughput = "+str(max(ls))
				print "Minimum Throughput = "+str(min(ls))
				print "Average Throughput = "+str(sum(ls)/len(ls))

	def readFile(self,fname):
		with open(fname) as f:
			content = [x.strip('\n') for x in f.readlines()]
		t = Transactions(self.id)
		i = 0
		tcount = 0
		while i < len(content):
			transx = content[i].split(",");
			transType = transx[0]
			tcount =  tcount + 1
			if transType == 'N':
				M = int(transx[4])
				c = int(transx[1])
				w = int(transx[2])
				d = int(transx[3])
				lis = content[i+1:i+M+1]
				t.neworder(c,w,d,lis)
				i = i + M
			elif transType == 'D':
				w = int(transx[1])
				c = int(transx[2])
				t.delivery(w,c)
			elif transType == 'P':
				w = int(transx[1])
				d = int(transx[2])
				c = int(transx[3])
				p = int(float(transx[4]))
				t.payment(c,w,d,p)
			elif transType == 'S':
				w = int(transx[1])
				d = int(transx[2])
				th  = int(transx[3])
				l = int(transx[4])
				t.stocklevel(w,d,th,l)
			elif transType == 'O':
				w = int(transx[1])
				d = int(transx[2])
				c = int(transx[3])
				t.orderstatus(w,d,c)
			elif transType == 'I':
				w = int(transx[1])
				d = int(transx[2])
				l = int(transx[3])
				t.popularItem(w,d,l)
			elif transType == 'T':
				t.topbalance()
			else:
				print 'Input Mistmatch'
			i = i+1;
		return tcount;

class MainThread:
        totaltime =0.0
        totalcount =0
        starttime = 0.0
        totalfiles =0
        filesfinish = 0


