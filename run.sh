echo "Inserting Data into the tables"
echo "-----------------------------------------------------------------------------------------------"
python mergeCWD.py
python mergeItemStock.py
python mergeOrders.py
cqlsh -e "copy killrvideo.randoms(id,name) from 'test.csv' with header = false;"

echo "Data inserted Successfully"
echo "-----------------------------------------------------------------------------------------------"

