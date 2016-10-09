from multiprocessing import Pool,Process
import time
from client import MyThread, MainThread
import sys
import os.path


if __name__ == '__main__':
    MainThread.starttime = time.time()
    start_time = time.time()
    begin = int(sys.argv[1])
    end = int(sys.argv[2])

    totalfiles = end + 1 - begin
    countfile = 'count.txt'
    ls = []
#     ready_count = 0 
#     files = [i for i in xrange(begin, end+1)]
#     pool = Pool(totalfiles)
#     result = pool.map_async(MyThread().run, files)
    if os.path.isfile(countfile):	
	os.remove(countfile)

    for i in xrange(begin, end+1):
	thread = MyThread()
	process = Process(target=thread.run, args=(i,))
	process.start()
        # result = pool.map_async(thread.run,i)
	
	ls.append(process)
    
    for i in ls:
	i.join()

    end_time = time.time()
    timetaken = end_time - start_time
    thru = []
    with open(countfile, 'r') as f:
	content = [x.strip('\n') for x in f.readlines()] 
	count= 0
        for c in content:
	    thru.append(float(c.split(',')[1]))
            count += int(c.split(',')[0])
	print "Max:",max(thru)
	print "Min:",min(thru)
	print "Avg:",sum(thru)/len(thru)
	print "Total Throughpupt",count/timetaken
