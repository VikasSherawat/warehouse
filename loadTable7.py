import pandas as pd
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
import copy
import datetime
from decimal import Decimal

cluster = Cluster(contact_points = ['127.0.0.1'])
print(cluster)
session = cluster.connect('warehouse')

#This is to load data in table 7 with counter since it can not be copied be directly copied from csv files
contents =[]
with open('table7.csv') as f:
        for x in f.readlines():
                if x not in contents:
                        contents.append(x)

i=1
while i < len(contents):
        transx = contents[i].strip('\n').split(",");
        query = 'UPDATE c_payment SET c_balance = c_balance +'+str(int(float(transx[3])))+ ' WHERE w_id='+ transx[0] +' and d_id='+ transx[1] +' and c_id='+ transx[2]+';'
        session.execute(query)
        query = 'UPDATE c_payment SET c_ytd_payment = c_ytd_payment +'+str(int(float(transx[4])))+ ' WHERE w_id='+ transx[0] +' and d_id='+ transx[1] +' and c_id='+ transx[2]+';'
        session.execute(query)
        query = 'UPDATE c_payment SET c_payment_cnt = c_payment_cnt +'+str(int(float(transx[5])))+ ' WHERE w_id='+ transx[0] +' and d_id='+ transx[1] +' and c_id='+ transx[2]+';'
        session.execute(query)
        query = 'UPDATE c_payment SET c_delivery_cnt = c_delivery_cnt +'+transx[6]+ ' WHERE w_id='+ transx[0] +' and d_id='+ transx[1] +' and c_id='+ transx[2]+';'
        session.execute(query)
        i=i+1

