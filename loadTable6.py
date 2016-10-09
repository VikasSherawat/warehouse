from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
import copy
import datetime
from decimal import Decimal
from cass import DB

db = DB()
session = db.getInstance(['127.0.0.1'],9042,'warehouse')

#This is to load data in table 5 with counter since it can not be copied be directly copied from csv files
contents =[]
with open('table6.csv') as f:
        for x in f.readlines():
                if x not in contents:
                        contents.append(x)

i=1
while i < len(contents):
        transx = contents[i].strip('\n').split(",");
        query = 'UPDATE d_payment SET d_ytd = d_ytd +'+str(int(float(transx[2]))) + ' WHERE w_id='+ transx[0] +' and d_id='+ transx[1] +';'
        session.execute(query)
        i=i+1

