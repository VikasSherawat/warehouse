echo "Merging data"
echo "-----------------------------------------------------------------------------------------------"
python mergeCWD.py
python mergeItemStock.py
python mergeOrders.py

echo "-----------------------------------------------------------------------------------------------"
echo "Creating tables"
echo "-----------------------------------------------------------------------------------------------"
python createTables.py

echo "-----------------------------------------------------------------------------------------------"
echo "Tables created in cassandra"
echo "-----------------------------------------------------------------------------------------------"

echo "Inserting Data into the tables"
echo "-----------------------------------------------------------------------------------------------"

echo "Inserting Data into the tables 1,2,3 and 6:"
cqlsh -f 'copyFromCSV.cql'

echo "Inserting Data into table 4: next_order"
python loadTable4.py

echo "Inserting Data into table 5: wd_payment"
python loadTable5.py

echo "Inserting Data into table 7: c_payment"
python loadTable7.py

echo "-----------------------------------------------------------------------------------------------"
echo "Data inserted successfully"
echo "-----------------------------------------------------------------------------------------------"


