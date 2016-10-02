import pandas as pd

file1 = pd.read_csv("order.csv")
file2 = pd.read_csv("order-line.csv")
file3 = pd.merge(file1, file2, left_on=['O_W_ID','O_D_ID','O_ID'], right_on=['OL_W_ID','OL_D_ID','OL_O_ID'])
file3.drop(['OL_W_ID','OL_D_ID','OL_O_ID'], axis=1, inplace=True)
file3.rename(columns={'O_W_ID':'W_ID','O_D_ID':'D_ID'}, inplace=True)

file4 = file3.drop(['O_CARRIER_ID'],axis=1)
file4.to_csv("table2.csv", index= False)

file3.to_csv("table6.csv", index = False, columns = ['W_ID','D_ID','O_ID','O_CARRIER_ID'])

