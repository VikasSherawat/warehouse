echo "Inserting Data into the tables"
echo "-----------------------------------------------------------------------------------------------"
python mergeCWD.py
python mergeItemStock.py
python mergeOrders.py

echo "Data inserted Successfully"
echo "-----------------------------------------------------------------------------------------------"

