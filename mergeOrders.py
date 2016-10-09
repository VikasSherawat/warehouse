import pandas as pd

file1 = pd.read_csv("order.csv")
file2 = pd.read_csv("order-line.csv")
file3 = pd.read_csv("item.csv")
file4 = pd.merge(pd.merge(file1, file2, left_on=['O_W_ID','O_D_ID','O_ID'], right_on=['OL_W_ID','OL_D_ID','OL_O_ID']),file3,
left_on=['OL_I_ID'],right_on=['I_ID'],how='left')

file4.drop(['OL_W_ID','OL_D_ID','OL_O_ID','I_ID','I_IM_ID','I_DATA'], axis=1, inplace=True)
file4.rename(columns={'O_W_ID':'W_ID','O_D_ID':'D_ID','I_NAME':'OL_I_NAME','I_PRICE':'OL_I_PRICE'}, inplace=True)


file4.to_csv("table2.csv", index= False)

file4.to_csv("table7.csv", index = False, columns = ['W_ID','D_ID','O_ID','O_CARRIER_ID'])

