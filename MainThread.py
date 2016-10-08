import threading
import time
from client import MyThread,MainThread
import sys
import os.path


if __name__ == "__main__":
        #print 'Inside main'
        MainThread.starttime = time.time()
	begin = int(sys.argv[1])
        end = int(sys.argv[2])
	MainThread.totalfiles = end+1-begin
	if os.path.isfile('result.txt'):
		os.remove('result.txt')
	ls = []
	for i in xrange(begin,end+1):
		t = MyThread(i)
		ls.append(t)
		t.start()
	for i in ls:
		i.join()
