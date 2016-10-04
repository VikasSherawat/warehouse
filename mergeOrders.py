import pandas as pd
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
import copy
import datetime
from decimal import Decimal

cluster = Cluster(contact_points = ['127.0.0.1'])
print(cluster)
session = cluster.connect('warehouse')

file1 = pd.read_csv("order.csv")
file2 = pd.read_csv("order-line.csv")
file3 = pd.merge(file1, file2, left_on=['O_W_ID','O_D_ID','O_ID'], right_on=['OL_W_ID','OL_D_ID','OL_O_ID'])
file3.drop(['OL_W_ID','OL_D_ID','OL_O_ID'], axis=1, inplace=True)
file3.rename(columns={'O_W_ID':'W_ID','O_D_ID':'D_ID'}, inplace=True)

file4 = file3.drop(['O_CARRIER_ID'],axis=1)
file4.to_csv("table2.csv", index= False)

file3.to_csv("table6.csv", index = False, columns = ['W_ID','D_ID','O_ID','O_CARRIER_ID'])

insert_table2 = session.prepare("INSERT INTO orderline (w_id,d_id,o_id,o_c_id,o_ol_cnt,o_all_local,o_entry_d,ol_number,ol_i_id,ol_delivery_d,ol_amount,ol_supply_w_id,ol_quantity,ol_dist_info) VALUES ( ?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
insert_table2.consistency_level = 1

batch = BatchStatement()
#batch = BatchStatement(consistency_level = ConsistencyLevel.QUORUM)
batch = BatchStatement(consistency_level = 1)
contents =[]
df = pd.read_csv("table2.csv")
df[['O_ENTRY_D','OL_DELIVERY_D']] = df[['O_ENTRY_D','OL_DELIVERY_D']].astype('datetime64')
data = df.values.tolist()
for x in data:
        #print(list(x))
        if x not in contents:
                contents.append(x)
                batch.add(insert_table2, (x))
session.execute(batch)

############################

insert_table6 = session.prepare("INSERT INTO o_carrier (w_id, d_id,o_id,o_carrier_id) VALUES ( ?,?,?,?)")
insert_table6.consistency_level = 1

batch = BatchStatement()
#batch = BatchStatement(consistency_level = ConsistencyLevel.QUORUM)
batch = BatchStatement(consistency_level = 1)
contents =[]
df = pd.read_csv("table6.csv")
data = df.values.tolist()
for x in data:
        #print(list(x))
        if x not in contents:
                contents.append(x)
                batch.add(insert_table6, (x))
session.execute(batch)
