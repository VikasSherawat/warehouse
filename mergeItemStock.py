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

