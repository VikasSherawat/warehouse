import threading
import time
from client import MyThread
import sys




if __name__ == "__main__":
        #print 'Inside main'
        start = time.time()
        end = int(sys.argv[1])	
	for i in xrange(0,end+1):
		t = MyThread(i)
		t.start()
