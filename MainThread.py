import threading
import time
from client import MyThread
import sys
import os.path



if __name__ == "__main__":
        #print 'Inside main'
        start = time.time()
        end = int(sys.argv[1])
	if os.path.isfile('result.txt'):
		os.remove('result.txt')
	for i in xrange(0,end+1):
		t = MyThread(i)
		t.start()
