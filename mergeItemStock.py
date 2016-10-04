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

file1 = pd.read_csv("item.csv")
file2 = pd.read_csv("stock.csv")
file3 = pd.merge(file1, file2, left_on=['I_ID'], right_on=['S_I_ID'])
file3.drop(['S_I_ID'], axis=1, inplace=True)
file3.rename(columns={'S_W_ID':'W_ID'}, inplace=True)
file3.to_csv("table3.csv", index= False)

insert_table3 = session.prepare("INSERT INTO item_stock (i_id,i_name,i_price,i_im_id,i_data,w_id,s_quantity,s_ytd,s_order_cnt,s_remote_cnt,s_dist_01,s_dist_02,s_dist_03,s_dist_04,s_dist_05,s_dist_06,s_dist_07,s_dist_08,s_dist_09,s_dist_10,s_data) VALUES (?, ?, ?, ?,?, ?, ?, ?,?, ?, ?, ?,?, ?, ?, ?,?, ?, ?, ?,?)")
insert_table3.consistency_level = 1


batch = BatchStatement()
#batch = BatchStatement(consistency_level = ConsistencyLevel.QUORUM)
batch = BatchStatement(consistency_level = 1)
contents =[]

data = pd.read_csv("table3.csv").values.tolist()

for x in data:
        #print(list(x))
        if x not in contents:
                contents.append(x)
		print(x)
                batch.add(insert_table3, x)

session.execute(batch)
