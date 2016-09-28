from cass import DB
from cassandra.cluster import Cluster
from popularitem import PopularItem,ItemInfo
import copy

class Transactions:
	def __init__(self):
		db = DB()
		self.session = db.getInstance(['127.0.0.1'],9042,'KillrVideo')
	
	def neworder(self,c,w,d,lis):
	        print "new Order() = ",c
        	#print "Warehouse ID = ",w
        	#print "District ID = ",d
        	#print "Items ordered are:\n"
        	#for item in lis:
                	#print item
        	#print "\n"

	def stocklevel(self,w,d,t,l):
		print "Stock  Level() ",w
		s = self.session
                query  = 'Select d_next_oid from next_order where w_id='+str(w)+' and d_id='+str(d)
		rows = s.execute(query)
		oid = 0
		for row in rows:
			o_id = int(row.d_next_oid)
		oid = oid-l
		query = 'select ol_i_id from orderline where w_id='+str(w)+' and d_id='+str(d)+' and o_id >='+str(oid)
		rows = s.execute(query)
		print 'Fetched  order:'
		count = 0
		for row in rows:
			item_id = int(row.ol_i_id)
			query = 'select s_quantity from item_stock where w_id ='+str(w)+' and i_id ='+str(item_id)
			result = s.execute(query)
			for j in result:
				if j.s_quantity <t:
					count = count+1
		print "Total number of items in warehouse:"+str(w)+" below threshold is: "+str(count)
	
	def popularItem(self,w,d,l):
		s = self.session
		query  = 'Select d_next_oid from next_order where w_id='+str(w)+' and d_id='+str(d)
                rows = s.execute(query)
                oid = 0
                for row in rows:
                        o_id = int(row.d_next_oid)
                oid = oid-l
                query = 'select o_id,ol_i_id,o_entry_d,ol_quantity,c_id from orderline where w_id='+str(w)+' and d_id='+str(d)+' and o_id >='+str(oid)
                rows = s.execute(query)
		storerows = copy.copy(rows)
		orderdc = dict()
		for row in rows:
			item = row.ol_i_id
			oid = row.o_id
			cid = row.c_id
			quan = row.ol_quantity
			entry_d = row.o_entry_d
			if oid in orderdc:
				pItem = orderdc[oid]
				if pItem.quantity < quan:
					pItem.item = item
					pItem.quantity = quan
					pItem.entry = entry_d
					orderdc[oid] = pItem

			else:
				pItem = PopularItem(oid,item,quan,entry_d,cid)
				orderdc[oid] = pItem	

		#after getting all the popular item in the last L order
		print "District Identifier:(",w,",",d,")"
		print "Orders Examined:",l
		
		itemdc = dict()

		for obj in orderdc.itervalues():
			item = obj.item
			cid = obj.cid
			query = 'select c_first,c_middle,c_last from customer_main where w_id = '+str(w)+' and d_id='+str(d)+' and c_id='+str(cid)
			result = s.execute(query)
			first = ""
			middle = ""
			last = ""
			for r in result:
				first = r.c_first
				middle = r.c_middle
				last = r.c_last
			query = 'select i_name from item_stock where w_id ='+str(w)+' and i_id ='+str(item)
			result = s.execute(query)
			iname = ""
			for r in result:
				iname = r.i_name
			
			if  item not in itemdc:
				print 'Setting the iteminfo object for: ',iname
				ob = ItemInfo(item,iname,0)
				itemdc[item] = ob
			
			print 'Order Id and Entry Date:',obj.oid,",",obj.entry
			print 'Customers who placed the order:',first,middle,last
			print 'Item name and quantity of popular item:',iname,",",obj.quantity
		
		for row in storerows:
			item = row.ol_i_id
			print 'Checking the item: ',item
			if item in itemdc:
				obj = itemdc[item]
				obj.count = obj.count+1
				print 'Increase count of ',item,'. New count is: ',obj.count
		for j in itemdc.itervalues():
			print j.name,(j.count*100)/l,'%'
			

	def topbalance(self):
		s = self.session
		print 'Top 10 Customers with maximum outstanding balance are:'
		query = 'select w_id,d_id,c_id,c_balance from c_payment limit 10'
		rows = s.execute(query)
		for row in rows:
			wid = row.w_id
			did = row.d_id
			cid = row.c_id
			query = 'select c_first,c_middle,c_last,w_name,d_name from customer_main where w_id = '+str(wid)+' and d_id='+str(did)+' and c_id='+str(cid)
			r = s.execute(query)
			first = "vikas"
			middle = ""
			last = "Sherawat"
			wname = "Warehouse"
			dname = "district"
			for s in r:
				first = s.c_first
				middle = s.c_middle
				last = s.c_last
			print row.c_balance
