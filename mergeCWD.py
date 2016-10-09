import pandas as pd

file1 = pd.read_csv("warehouse.csv")
file2 = pd.read_csv("district.csv")
file3 = pd.read_csv("customer.csv")
file4 = pd.merge(pd.merge(file1, file2, left_on=['W_ID'], right_on=['D_W_ID']),file3, right_on=['C_W_ID','C_D_ID'], left_on=['W_ID','D_ID'])
df = file4.drop(['D_W_ID','C_W_ID','C_D_ID'], axis=1, inplace=True)

file5 = file4.drop(['W_YTD','D_YTD','D_NEXT_O_ID','C_BALANCE','C_YTD_PAYMENT','C_PAYMENT_CNT','C_DELIVERY_CNT'],axis=1)
file5.to_csv("table1.csv", index= False)

file4.to_csv("table4.csv", index = False, columns = ['W_ID','D_ID','D_NEXT_O_ID'])

file4.to_csv("table5.csv", index = False, columns = ['W_ID','W_YTD'])

file4.to_csv("table6.csv", index = False, columns = ['W_ID','D_ID','D_YTD'])

file4.to_csv("table8.csv", index = False, columns = ['W_ID','D_ID','C_ID','C_YTD_PAYMENT','C_PAYMENT_CNT','C_DELIVERY_CNT'])

file4['DC_ID']=file4['C_ID']
file4.to_csv("table9.csv", index = False, columns = ['W_ID','D_ID','C_BALANCE','DC_ID','C_ID'])
