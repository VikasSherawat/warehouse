import pandas as pd
import numpy as np
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
import copy
import datetime
from decimal import Decimal

cluster = Cluster(contact_points = ['127.0.0.1'])
print(cluster)
session = cluster.connect('warehouse')

file1 = pd.read_csv("warehouse.csv")
file2 = pd.read_csv("district.csv")
file3 = pd.read_csv("customer.csv")
file4 = pd.merge(pd.merge(file1, file2, left_on=['W_ID'], right_on=['D_W_ID']),file3, right_on=['C_W_ID','C_D_ID'], left_on=['W_ID','D_ID'])
file4.drop(['D_W_ID','C_W_ID','C_D_ID'], axis=1, inplace=True)

file5 = file4.drop(['W_YTD','D_YTD','D_NEXT_O_ID','C_BALANCE','C_YTD_PAYMENT','C_PAYMENT_CNT','C_DELIVERY_CNT'],axis=1)
file5.to_csv("table1.csv", index= False)

file4.to_csv("table4.csv", index = False, columns = ['W_ID','D_ID','D_NEXT_O_ID'])

file4.to_csv("table5.csv", index = False, columns = ['W_ID','D_ID','W_YTD','D_YTD'])

file4.to_csv("table7.csv", index = False, columns = ['W_ID','D_ID','C_ID','C_BALANCE','C_YTD_PAYMENT','C_PAYMENT_CNT','C_DELIVERY_CNT'])

insert_table1 = session.prepare("INSERT INTO customer_main (w_id,w_name,w_street_1,w_street_2,w_city,w_state,w_zip,w_tax,d_id,d_name,d_street_1,d_street_2,d_city,d_state,d_zip,d_tax,c_id,c_first,c_middle,c_last,c_street_1,c_street_2,c_city,c_state,c_zip,c_phone,c_since,c_credit,c_credit_lim,c_discount,c_data) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

insert_table1.consistency_level = 1

batch = BatchStatement()
#batch = BatchStatement(consistency_level = ConsistencyLevel.QUORUM)
batch = BatchStatement(consistency_level = 1)
contents =[]
df = pd.read_csv("table1.csv")
df[['C_SINCE']] = df[['C_SINCE']].astype('datetime64[ns]')
df[['W_ZIP','D_ZIP','C_ZIP','C_PHONE']]= df[['W_ZIP','D_ZIP','C_ZIP','C_PHONE']].astype('string')
data = df.values.tolist()
for x in data:
        print(x)
        if x not in contents:
                contents.append(x)
                batch.add(insert_table1, (x))
session.execute(batch)

#This is to load data in table 4 with counter since it can not be copied be directly copied from csv files
contents =[]
with open('table4.csv') as f:
        for x in f.readlines():
                if x not in contents:
                        contents.append(x)

i=1
while i < len(contents):
        transx = contents[i].strip('\n').split(",");
        query = 'UPDATE next_order SET d_next_o_id=d_next_o_id+'+transx[2]+ ' WHERE w_id='+ transx[0] +' and d_id='+ transx[1] +';'
	print(query)
        session.execute(query)
	i=i+1

#This is to load data in table 5 with counter since it can not be copied be directly copied from csv files
contents =[]
with open('table5.csv') as f:
        for x in f.readlines():
                if x not in contents:
                        contents.append(x)

i=1
while i < len(contents):
        transx = contents[i].strip('\n').split(",");
	query = 'UPDATE wd_payment SET w_ytd = w_ytd +'+ str(int(float(transx[2]))) + ' WHERE w_id='+ transx[0] +' and d_id='+ transx[1] +';'
	print(query)
        session.execute(query)
	query = 'UPDATE wd_payment SET d_ytd = d_ytd +'+str(int(float(transx[3]))) + ' WHERE w_id='+ transx[0] +' and d_id='+ transx[1] +';'
	print(query)
        session.execute(query)
	i=i+1

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
