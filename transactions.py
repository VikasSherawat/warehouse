from cass import DB
from cassandra.cluster import Cluster
from popularitem import PopularItem,ItemInfo
import copy
import datetime


class Transactions:
	def __init__(self):
		db = DB()
		self.session = db.getInstance(['127.0.0.1'],9042,'KillrVideo')
	
	def neworder(self,c,w,d,lis):
	        print "new Order() = ",c
        	print "Warehouse ID = ",w
        	print "District ID = ",d
        	print "Items ordered are:\n"
		count = 1
		all_local = 1
		for item in lis:
			if w != item[1]
				all_local = 0
				break;
        	query = s.prepare("Insert into orderline (w_id ,d_id ,o_id ,ol_number ,c_id ,o_ol_cnt ,o_all_local ,o_entry_d ,ol_i_id ,ol_supply_w_id ,ol_delivery_d,ol_amount, ol_quantity ,ol_dist_info) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
		
		batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
			
		u_batch = BatchStatement()

		total_amount = 0.0
		entry_d = datetime.datetime.now()
		q  = 'Select d_next_oid from next_order where w_id='+str(w)+' and d_id='+str(d)
		rows = s.execute(q)
		order = 0
		for row in rows:
			order = int(row.d_next_oid)

		for item in lis:	
			q = "select i_price,s_quantity,s_ytd,s_order_cnt,s_remote_cnt from item_stock where w_id = "+str(item[1])+' and i_id ='+str(item[0])
			rows = s.execute(q)
			amount = 0
			s_quantity = 0
			s_ytd = 0.0
			order_cnt = 0
			remote_cnt = 0

			for row in rows:
				amount  = row.i_price
				s_quantity = row.s_quantity
				s_ytd = row.s_ytd+item[2]
				order_cnt = row.s_order_cnt+1
				remote_cnt = item[1]==w?row.s_remote_cnt:row.s_remote_cnt+1

			a_quantity = s_quantity-item[2]
			if a_quantity < 10:
				a_quantity = a_quantity + 100

			q = s.prepare("update item_stock set s_quantity = ?,s_ytd = ?,s_order_cnt = ?,s_remote_cnt = ? where w_id = ? and i_id = ?")
		
			u_batch.add(q,(a_quantity,s_ytd,order_cnt,remote_cnt,item[1],item[0]))
			w_id =w
			d_id =d
			o_id =order
			ol_number = count
			c_id = c
			o_ol_cnt =len(lis)
			o_all_local =all_local
			o_entry_d = entry_d
			ol_i_id =item[0]
			ol_supply_w_id =item[1]
			ol_delivery_d=null
			ol_quantity =item[2]
			dist = "S_DIST_"+str(d)
			ol_dist_info = dist
			item_amount = amount*item[2]
			ol_amount=item_amount
			total_amount = total_amount + item_amount
			batch.add(query,(w_id ,d_id ,o_id ,ol_number ,c_id ,o_ol_cnt ,o_all_local ,o_entry_d ,ol_i_id ,ol_supply_w_id ,ol_delivery_d,ol_amount, ol_quantity ,ol_dist_info))
		s.execute(batch)
		s.execute(ubatch)
		q = 'update next_order set d_next_oid = d_next_oid + 1 where w_id='+str(w)+' and d_id='+str(d)
		rows = s.execute(q)
		
		query = 'select c_last,c_credit,c_discount,w_tax,d_tax where w_id='+str(w)+' and d_id='+str(d)' and c_id ='+str(c)
		rows = s.execute(query)

		lname = ""
		credit = ""
		discount = 0.0
		wtax = 0.0
		dtax = 0.0
 
		for row in rows:
			lname = row.c_last
			credit = row.c_credit
			discount = row.c_discount
			wtax = row.w_tax
			dtax = row.d_tax
		total_amount = total_amount * (1+wtax+dtax)  * (1-discount)
		print "Customer Identifier: "w,d,c,lname,credit,discount
		print "Warehouse Tax = ",wtax," and District Tax = ",dtax
		print "Order Number = ",order,", Entry Date = ",entry_d
		print "Number of Items: ",len(lis)," and total amount = ",total_amount
		print 
		

        	print "\n"

	def stocklevel(self,w,d,t,l):
		s = self.session
                query  = 'Select d_next_oid from next_order where w_id='+str(w)+' and d_id='+str(d)
		rows = s.execute(query)
		oid = 0
		for row in rows:
			o_id = int(row.d_next_oid)
		oid = oid-l
		query = 'select ol_i_id from orderline where w_id='+str(w)+' and d_id='+str(d)+' and o_id >='+str(oid)
		rows = s.execute(query)
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
				ob = ItemInfo(item,iname,0)
				itemdc[item] = ob
			
			print 'Order Id and Entry Date:',obj.oid,",",obj.entry
			print 'Customers who placed the order:',first,middle,last
			print 'Item name and quantity of popular item:',iname,",",obj.quantity
		
		for row in storerows:
			item = row.ol_i_id
			if item in itemdc:
				obj = itemdc[item]
				obj.count = obj.count+1
		for j in itemdc.itervalues():
			print j.name,(j.count*100)/l,'%'
			

	def topbalance(self):
		s = self.session
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
			print first,middle,last,row.c_balance,wname,dname
