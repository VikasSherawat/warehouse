echo "Merging data"
echo "-----------------------------------------------------------------------------------------------"
#export PATH="/temp/anaconda2/bin:$PATH"
#python mergeCWD.py
#python mergeItemStock.py
#python mergeOrders.py

#export PATH="echo $PATH | sed -e 's/:\/temp/anaconda2/bin/$//'"
echo "-----------------------------------------------------------------------------------------------"
cqlsh -e "CREATE KEYSPACE warehouse WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;"
echo "Creating tables"
echo "-----------------------------------------------------------------------------------------------"
#python createTables.py

echo "-----------------------------------------------------------------------------------------------"
echo "Tables created in cassandra"
echo "-----------------------------------------------------------------------------------------------"

echo "Inserting Data into the tables"
echo "-----------------------------------------------------------------------------------------------"

echo "Inserting Data into the tables 1,2,3,7 and 9:"
cqlsh -f 'copyFromCSV.cql' 

echo "Inserting Data into table 4: next_order"
python loadTable4.py

echo "Inserting Data into table 5: w_payment"
python loadTable5.py

echo "Inserting Data into table 6: d_payment"
python loadTable6.py

echo "Inserting Data into table 8: c_payment"
python loadTable8.py

echo "Inserting Data into table 9: c_balance"
python loadTable9.py


echo "-----------------------------------------------------------------------------------------------"
echo "Data inserted successfully"
echo "-----------------------------------------------------------------------------------------------"


