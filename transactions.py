from cass import DB
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement,SimpleStatement
from popularitem import PopularItem,ItemInfo,NewOrder
import copy
import datetime
from decimal import Decimal

class Transactions:
	def __init__(self):
		db = DB()
		self.session = db.getInstance(['127.0.0.1'],9042,'KillrVideo')
	
	def neworder(self,c,w,d,lis):
		s = self.session
		count = 1
		all_local = 1
		for j in lis:
			item = j.split(",")
			if not w != item[1]:
				all_local = 0
				break
        	query = s.prepare("Insert into orderline (w_id ,d_id ,o_id ,ol_number ,c_id ,o_ol_cnt ,o_all_local ,o_entry_d ,ol_i_id ,ol_supply_w_id ,ol_amount, ol_quantity ,ol_dist_info) values (?,?,?,?,?,?,?,?,?,?,?,?,?)")
		
		batch = BatchStatement()
			
		u_batch = BatchStatement()

		total_amount = Decimal(0)
		entry_d = datetime.datetime.now()
		q  = 'Select d_next_oid from next_order where w_id='+str(w)+' and d_id='+str(d)
		rows = s.execute(q)
		order = 0
		for row in rows:
			order = int(row.d_next_oid)
		orderList = []
		for j in lis:
			item = j.split(",")
			i_id = int(item[0])	
			sw_id = int(item[1])	
			i_quant = int(item[2])
			q = "select i_name,i_price,s_quantity,s_ytd,s_order_cnt,s_remote_cnt from item_stock where w_id = "+str(sw_id)+' and i_id ='+str(i_id)
			rows = s.execute(q)
			name = ""
			amount = 0.0
			s_quantity = 0
			s_ytd = 0.0
			order_cnt = 0
			remote_cnt = 0
			for row in rows:
				name = row.i_name
				amount  = row.i_price
				s_quantity = row.s_quantity
				s_ytd = row.s_ytd + i_quant
				order_cnt = row.s_order_cnt + 1
				remote_cnt = row.s_remote_cnt if sw_id == w else row.s_remote_cnt+1

			a_quantity = s_quantity-i_quant
			if a_quantity < 10:
				a_quantity = a_quantity + 100

			q = s.prepare("update item_stock set s_quantity = ?,s_ytd = ?,s_order_cnt = ?,s_remote_cnt = ? where w_id = ? and i_id = ?")
			u_batch.add(q,(a_quantity,s_ytd,order_cnt,remote_cnt,sw_id,i_id))
			w_id =w
			d_id =d
			o_id =order
			ol_number = count
			count = count+1
			c_id = c
			o_ol_cnt =len(lis)
			o_all_local =all_local
			o_entry_d = entry_d
			ol_i_id =i_id
			ol_supply_w_id =sw_id
			ol_quantity =i_quant
			dist = "S_DIST_"+str(d)
			ol_dist_info = dist
			item_amount = amount * i_quant
			ol_amount=item_amount
			newOrder = NewOrder(i_id,name,sw_id,i_quant,ol_amount,a_quantity)
			orderList.append(newOrder)
			total_amount = total_amount + item_amount
			batch.add(query,(w_id ,d_id ,o_id ,ol_number ,c_id ,o_ol_cnt ,o_all_local ,o_entry_d ,ol_i_id ,ol_supply_w_id,ol_amount, ol_quantity ,ol_dist_info))
		s.execute(batch)
		s.execute(u_batch)
		q = 'update next_order set d_next_oid = d_next_oid + 1 where w_id='+str(w)+' and d_id='+str(d)
		rows = s.execute(q)
		
		query = 'select c_last,c_credit,c_discount,w_tax,d_tax from customer_main where w_id='+str(w)+' and d_id='+str(d)+' and c_id ='+str(c)
		rows = s.execute(query)
		
		query = s.prepare('insert into o_carrier(w_id,d_id,o_id,o_carrier_id) values (?,?,?,?)')
		cbatch = BatchStatement()
		cbatch.add(query,(w,d,order,-1))
		s.execute(cbatch)
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
		print "Customer Identifier: ",w,d,c,lname,credit,discount
		print "Warehouse Tax = ",wtax," and District Tax = ",dtax
		print "Order Number = ",order,", Entry Date = ",entry_d
		print "Number of Items: ",len(lis)," and total amount = ",total_amount
		for obj in orderList:
			print obj.itemid,",",obj.name,",",obj.sw_id,",",obj.quantity,",",obj.amount,",",obj.stock

	def delivery(self,w,carrier):
		s = self.session
		orderdc = {}
		for i in range(1,11):
			query = 'select o_id from o_carrier where w_id='+str(w)+' and d_id='+str(i)+' and o_carrier_id = -1  limit 1'
			rows = s.execute(query)
			for row in rows:
				orderdc[i] = row.o_id
		print 'Order Dictionary{d_id:o_id} :',orderdc
		
		ubatch = BatchStatement()
		d_date = datetime.datetime.now()
		obatch = BatchStatement()
		for d_id,o_id in orderdc.iteritems():
			query = 'update o_carrier set o_carrier_id ='+str(carrier)+ ' where w_id ='+str(w)+'  and d_id ='+str(d_id)+' and o_id ='+str(o_id)
			ubatch.add(query)
			c_query = 'select c_id,ol_amount,o_ol_cnt from orderline where w_id='+str(w)+' and d_id='+str(d_id)+' and o_id ='+str(o_id)
			rows = s.execute(c_query)
			c_id = 0
			ol_amount = Decimal(0)
			ol_count = 0
			for row in rows:
				ol_count = row.o_ol_cnt+1
				c_id = row.c_id
				ol_amount = ol_amount + row.ol_amount
			for i in range(1,ol_count):
				ubatch.add(SimpleStatement("update orderline set ol_delivery_d = %s where w_id = %s and d_id = %s and o_id= %s and ol_number= %s"),(d_date,w,d_id,o_id,i))

			query = 'select c_balance,c_delivery_cnt,c_payment_cnt, c_ytd_payment from c_payment where  w_id ='+str(w)+' and d_id ='+str(d_id)+' and c_id ='+str(c_id)
			result = s.execute(query)
			c_balance = Decimal(0)
			p_count = 0
			ytd = Decimal(0)
			d_count = 0
			for row in result:
				c_balance = row.c_balance+ol_amount
				d_count = row.c_delivery_cnt+1
				p_count = row.c_payment_cnt
				ytd = row.c_ytd_payment
			
			print w,d_id,c_id,c_balance,p_count,d_count,ytd

			 
			p_query = 'update c_payment set c_balance ='+str(c_balance)+' ,c_delivery_cnt ='+str(d_count)+'  where  w_id ='+str(w)+' and d_id ='+str(d_id)+' and c_id ='+str(c_id)
			ubatch.add(p_query)
		s.execute(ubatch)

		print 'Carrier_Id set for all orders:'	


	def orderstatus(self, w, d, c):
		s = self.session
		
		cq = 'select c_first, c_last, c_middle from customer_main where w_id='+str(w)+' and d_id = '+str(d)+' and c_id = '+ str(c)
		cresults = s.execute(cq)
		cfirst = ""
		clast = ""
		cmiddle = ""
		for result in cresults:
			cfirst = result.c_first
			clast = result.c_last
			cmiddle = result.c_middle
		
		cq2 = 'select c_balance from c_payment where w_id = '+ str(w) +' and d_id = '+ str(d) +' and c_id = '+ str(c)
		cresults2 = s.execute(cq2)

		cbalance = Decimal(0)
		for result in cresults2:
			cbalance = result.c_balance
	

		q = 'select o_id, o_entry_d from orderline where w_id = '+str(w)+' and d_id = '+str(d)+' and c_id = '+str(c)+' limit 1'
		print q
		results = s.execute(q)
		
		oid = 0
		oentryd = datetime.datetime.now()
		
		for result in results:
			oid = result.o_id
			oentryd = result.o_entry_d

		q2 = 'select o_carrier_id from o_carrier where w_id = '+ str(w) +' and d_id = '+ str(d) +' and o_id = '+ str(oid) 
		results2 = s.execute(q2)
		
		ocarrierid = 0
		
		for result in results2:
			ocarrierid = result.o_carrier_id

		

		print "1. : ",cfirst,clast,cmiddle,cbalance
		print "2. : ",oid,oentryd,ocarrierid

		q3 = 'select ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d from orderline where w_id ='+str(w)+' and d_id ='+str(d)+' and o_id = '+str(oid)
		results3 = s.execute(q3)
		
		iid = 0
		supplyid = 0
		olquantity = 0.0
		olamount = 0.0
		deliveryd = datetime.datetime.now()
		
		for result in results3:
			iid = result.ol_i_id
			supplyid = result.ol_supply_w_id
			olquantity = result.ol_quantity
			olamount = result.ol_amount
			deliveryd = result.ol_delivery_d
			print "3. : ",iid,supplyid,olquantity,olamount,deliveryd


	def stocklevel(self,w,d,t,l):
		s = self.session
                query  = 'Select d_next_oid from next_order where w_id='+str(w)+' and d_id='+str(d)
		rows = s.execute(query)
		oid = 0
		for row in rows:
			oid = int(row.d_next_oid)
		oid = oid-l
		query = 'select ol_i_id from orderline where w_id='+str(w)+' and d_id='+str(d)+' and o_id >='+str(oid)
		rows = s.execute(query)
		count = 0
		itemset = set()
		for row in rows:
			item_id = int(row.ol_i_id)
			if item_id in itemset:
				continue
			query = 'select s_quantity from item_stock where w_id ='+str(w)+' and i_id ='+str(item_id)
			result = s.execute(query)
			itemset.add(item_id)
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
                        oid = int(row.d_next_oid)
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
		query = 'select w_id,d_id,c_id,c_balance from c_payment'
		rows = s.execute(query)
		cust_list = []
		for row in rows:
			wid = row.w_id
			did = row.d_id
			cid = row.c_id
			bal = int(row.c_balance)
			ls = [wid,did,cid,bal]
			cust_list.append(ls)
		for i in sorted(cust_list,key=lambda x: x[3])[-10:][::-1]:
			wid,did,cid = i[0],i[1],i[2]
			query = 'select c_first,c_middle,c_last,w_name,d_name from customer_main where w_id = '+str(wid)+' and d_id='+str(did)+' and c_id='+str(cid)
                        print query
                        r = s.execute(query)
                        first = "vikas"
                        middle = ""
                        last = "Sherawat"
                        wname = "Warehouse"
                        dname = "district"
                        for res in r:
                                first = res.c_first
                                middle = res.c_middle
                                last = res.c_last
                        print first,middle,last,row.c_balance,wname,dname

