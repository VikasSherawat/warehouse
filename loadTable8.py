from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
import copy
import datetime
from decimal import Decimal
from cass import DB

db = DB()
session = db.getInstance(['127.0.0.1'],9042,'warehouse')

#This is to load data in table 8 with counter since it can not be copied be directly copied from csv files
contents =[]
with open('table8.csv') as f:
        for x in f.readlines():
                if x not in contents:
                        contents.append(x)

print( "running" )
i=1
while i < len(contents):
        transx = contents[i].strip('\n').split(",")
        w = transx[0]
        d = transx[1]
        c = transx[2]
        ytd = int(float(transx[3]))
        p_cnt = transx[4]
        d_cnt = transx[5]
        query = "update c_payment set c_ytd_payment = c_ytd_payment +" + str(int(float(transx[3]))) +", c_delivery_cnt = c_delivery_cnt +" + str(transx[4])+ ", c_payment_cnt = c_payment_cnt +" + str(transx[5])+ " where w_id =" + str(transx[0])+" and d_id ="+ str(transx[1])+ " and c_id =" + str(transx[2])+";"   
        session.execute(query)
        i=i+1
