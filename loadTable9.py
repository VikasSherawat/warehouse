from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
import copy
import datetime
from decimal import Decimal
from cass import DB

import uuid
db = DB()
session = db.getInstance(['127.0.0.1'],9042,'warehouse')

#This is to load data in table 8 with counter since it can not be copied be directly copied from csv files
contents =[]
print "Started"
with open('table9.csv') as f:
        for x in f.readlines():
                        contents.append(x)

print( "running" )
i=1
while i < len(contents):
        transx = contents[i].strip('\n').split(",")
        w = int(transx[0])
        d = int(transx[1])
        dc = int(transx[3])
	c = int(dc)
        bal = int(float(transx[2]))
	query = session.prepare("update c_balance set c_balance = c_balance + ? where w_id = ? and d_id =? and c_id = ?")
        session.execute(query,(bal,w,d,c))
        i=i+1
