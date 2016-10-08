from cass import DB
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement,SimpleStatement,BoundStatement
from popularitem import PopularItem,ItemInfo,NewOrder
import copy
import datetime
import os
from decimal import Decimal

class Transactions:
	def __init__(self,fid):
		db = DB()
		self.s = db.getInstance(['127.0.0.1'],9042,'warehouse')
		self.fname = str(fid)+'-output.txt'
                if os.path.isfile(self.fname):
                        os.remove(self.fname)
		self.file = open(self.fname,'a')
				
		#queries for new order Transaction
		self.n_i_order= self.s.prepare("Insert into orderline (w_id ,d_id ,o_id ,ol_number ,o_c_id ,o_ol_cnt ,o_all_local ,o_entry_d ,ol_i_id ,ol_supply_w_id ,ol_amount, ol_quantity ,ol_dist_info,o_carrier_id,o_i_name,o_i_price) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
		self.n_s_next  = self.s.prepare("Select d_next_o_id from next_order where w_id=? and d_id=?")
		self.n_s_item = self.s.prepare("select i_name,i_price,s_quantity,s_ytd,s_order_cnt,s_remote_cnt from item_stock where w_id = ? and i_id =?")
		self.n_u_item = self.s.prepare("update item_stock set s_quantity = ?,s_ytd = ?,s_order_cnt = ?,s_remote_cnt = ? where w_id = ? and i_id = ?")
		self.n_u_next = self.s.prepare("update next_order set d_next_o_id = d_next_o_id + 1 where w_id=? and d_id=?")
		self.n_s_cmain =self.s.prepare("select c_last,c_credit,c_discount,w_tax,d_tax from customer_main where w_id=? and d_id=? and c_id =?") 
		self.n_i_carr = self.s.prepare("insert into o_carrier(w_id,d_id,o_id,o_carrier_id) values (?,?,?,?)")

		#queries for Payment Transaction
		self.p_u_w_pay = self.s.prepare("update wd_payment set w_ytd = w_ytd + ? where w_id = ? and d_id = ?")
		self.p_u_d_pay = self.s.prepare("update wd_payment set d_ytd = d_ytd + ? where w_id = ? and d_id = ?")
		self.p_u_cpay = self.s.prepare("update c_payment set c_balance = c_balance - ?, c_ytd_payment = c_ytd_payment + ?, c_payment_cnt = c_payment_cnt + 1 where c_id = ? and w_id = ? and d_id = ?")
		self.p_s_cpay = self.s.prepare("select c_balance from c_payment where c_id=? and w_id=? and d_id=?")
		self.p_s_cmain = self.s.prepare("select c_first, c_last, c_middle, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, w_street_1, w_street_2, w_state, w_zip, w_city, d_street_1, d_street_2, d_city, d_state, d_zip from customer_main where c_id=? and w_id=?  and d_id=?")
		#queries for Delivery Transaction
		self.d_s_carr = self.s.prepare("select o_id from o_carrier where w_id=? and d_id=? and o_carrier_id = -1  limit 1")
		self.d_u_carr = self.s.prepare("update o_carrier set o_carrier_id =? where w_id =?  and d_id =? and o_id=?")
		self.d_s_order = self.s.prepare("select o_c_id,ol_amount,o_ol_cnt from orderline where w_id=? and d_id=? and o_id =?")
		self.d_u_order = self.s.prepare("update orderline set ol_delivery_d = ? where w_id = ? and d_id = ? and o_id= ? and ol_number=?")
		self.d_u_cpay = self.s.prepare("update c_payment set c_balance = c_balance + ?,c_delivery_cnt = c_delivery_cnt+1  where  w_id =? and d_id =? and c_id =?")

	
		#queries for OrderStatus Transaction
		self.o_s_cmain = self.s.prepare("select c_first, c_last, c_middle from customer_main where w_id=? and d_id=? and c_id=?")		
		self.o_s_cpay = self.s.prepare("select c_balance from c_payment where w_id = ? and d_id = ? and c_id = ?")
		self.o_s_order = self.s.prepare("select o_id, o_entry_d from orderline where w_id = ? and d_id = ? and o_c_id = ? limit 1")
		self.o_s_carr = self.s.prepare("select o_carrier_id from o_carrier where w_id = ? and d_id = ? and o_id = ?")
		self.o_s1_order = self.s.prepare("select ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d from orderline where w_id =? and d_id =? and o_id = ?")
		
		#queries for Stock Level Transaction
		self.s_s_next = self.s.prepare("Select d_next_o_id from next_order where w_id=? and d_id=?")
		self.s_s_order = self.s.prepare("select ol_i_id from orderline where w_id=? and d_id=? and o_id >=?")
		self.s_s_stock = self.s.prepare("select s_quantity from item_stock where w_id =? and i_id =?")
		

		#queries for Popular Item  Transaction
		self.i_s_next = self.s.prepare("Select d_next_o_id from next_order where w_id=? and d_id=?")
		self.i_s_order = self.s.prepare("select o_id,ol_i_id,o_entry_d,ol_quantity,o_c_id from orderline where w_id=? and d_id=? and o_id >=?")
		self.i_s_cmain = self.o_s_cmain
		self.i_s_stock = self.s.prepare("select i_name from item_stock where w_id =? and i_id =?")

	
		#queries for Top Balance Transaction	
		self.t_s_cpay = self.s.prepare("select w_id,d_id,c_id,c_balance from c_payment")
		self.t_s_cmain = self.s.prepare("select c_first,c_middle,c_last,w_name,d_name from customer_main where w_id = ? and d_id=? and c_id=?")

	
	def neworder(self,c,w,d,lis):
		s = self.s
		count = 1
		all_local = 1
		for j in lis:
			item = j.split(",")
			if not w != item[1]:
				all_local = 0
				break
		
		batch = BatchStatement()
			
		u_batch = BatchStatement()

		total_amount = Decimal(0)
		entry_d = datetime.datetime.now()
		rows = s.execute(self.n_s_next,(w,d))
		order = 0
		for row in rows:
			order = int(row.d_next_o_id)
		orderList = []
		for j in lis:
			item = j.split(",")
			i_id = int(item[0])	
			sw_id = int(item[1])	
			i_quant = int(item[2])
			
			rows = s.execute(self.n_s_item,(sw_id,i_id))
			name = ""
			amount = Decimal(0)
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

			u_batch.add(self.n_u_item,(a_quantity,s_ytd,order_cnt,remote_cnt,sw_id,i_id))
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
			if amount is None:
				amount = 0
			item_amount = Decimal(amount * i_quant)
			ol_amount=item_amount
			newOrder = NewOrder(i_id,name,sw_id,i_quant,ol_amount,a_quantity)
			orderList.append(newOrder)
			carrier = -1
			total_amount = total_amount + item_amount
			batch.add(self.n_i_order,(w_id ,d_id ,o_id ,ol_number ,c_id ,o_ol_cnt ,o_all_local ,o_entry_d ,ol_i_id ,ol_supply_w_id,ol_amount, ol_quantity ,ol_dist_info,carrier,name,amount))
		s.execute(batch)
		s.execute(u_batch)
		rows = s.execute(self.n_u_next,(w,d))
		
		rows = s.execute(self.n_s_cmain,(w,d,c))
		
		cbatch = BatchStatement()
		cbatch.add(self.n_i_carr,(w,d,order,-1))
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
		out = "Customer Identifier: "+str(w)+","+str(d)+","+str(c)+","+str(lname)+","+str(credit)+","+str(discount)+"\n"
		out += "Warehouse Tax = "+str(wtax)+" and District Tax = "+str(dtax)+"\n"
		out += "Order Number = "+str(order)+", Entry Date = "+str(entry_d)+"\n"
		out += "Number of Items:="+str(len(lis))+" and total amount = "+str(total_amount)+"\n"
		for obj in orderList:
			out += str(obj.itemid)+","+str(obj.name)+","+str(obj.sw_id)+","+str(obj.quantity)+","+str(obj.amount)+","+str(obj.stock)+"\n"
		self.file.write(out)



	def payment(self,c,w,d,payment):	
		s = self.s
		
		for i in range(1,11):
			s.execute(self.p_u_w_pay,(payment,w,i))
		s.execute(self.p_u_d_pay,(payment,w,d))
		s.execute(self.p_u_cpay,(payment,payment,c,w,d))
		rows = s.execute(self.p_s_cpay,(c,w,d))
		cbalance = 0.0
		for r in rows:
			cbalance = r.c_balance


		results = s.execute(self.p_s_cmain,(c,w,d))
		
		cfirst = ""
		clast = ""
		cmiddle = ""
		cstreet1 = ""
		cstreet2 = ""
		ccity = ""
		cstate = ""
		czip = ""
		cphone = ""
		csince = datetime.datetime.now()
		ccredit = ""
		ccreditlim = 0.0
		cdiscount = 0.0
		wstreet1 = ""
		wstreet2 = ""
		wcity = ""
		wstate = ""
		wzip = ""
		dstreet1 = ""
		dstreet2 = ""
		dcity = ""
		dstate = ""
		dzip = ""
		for result in results:
			cfirst = result.c_first
			clast = result.c_last
			cmiddle = result.c_middle
			cstreet1 = result.c_street_1
			cstreet2 = result.c_street_2
			ccity = result.c_city
			cstate = result.c_state
			czip = result.c_zip
			cphone = result.c_phone
			csince = result.c_since
			ccredit = result.c_credit
			ccreditlim = result.c_credit_lim
			cdiscount = result.c_discount
			wstreet1 = result.w_street_1
			wstreet2 = result.w_street_2
			wcity = result.w_city
			wstate = result.w_state
			wzip = result.w_zip
			dstreet1 = result.d_street_1
			dstreet2 = result.d_street_2
			dcity = result.d_city
			dstate = result.d_state
			dzip = result.d_zip

		out = str(w)+str(d)+str(c)+str(cfirst)+str(cmiddle)+str(clast)+str(cstreet1)+str(cstreet2)+str(ccity)+str(cstate)+str(czip)+str(cphone)+str(csince)+str(ccredit)+str(ccreditlim)+str(cdiscount)+str(cbalance)+"\n"
		out += str(wstreet1)+str(wstreet2)+str(wcity)+str(wstate)+str(wzip)+"\n"
		out += str(dstreet1)+str(dstreet2)+str(dcity)+str(dstate)+str(dzip)+"\n"
		out += str(payment)+"\n"
		self.file.write(out)

	def delivery(self,w,carrier):
		s = self.s
		orderdc = {}
		for i in range(1,11):
			#query = 'select o_id from o_carrier where w_id='+str(w)+' and d_id='+str(i)+' and o_carrier_id = -1  limit 1'
			rows = s.execute(self.d_s_carr,(w,i))
			for row in rows:
				orderdc[i] = row.o_id
		
		ubatch = BatchStatement()
		d_date = datetime.datetime.now()
		obatch = BatchStatement()
		for d_id,o_id in orderdc.iteritems():
			ubatch.add(self.d_u_carr,(carrier,w,d_id,o_id))
			rows = s.execute(self.d_s_order,(w,d_id,o_id))
			c_id = 0
			ol_amount = 0
			ol_count = 0
			for row in rows:
				ol_count = row.o_ol_cnt+1
				c_id = row.o_c_id
				ol_amount = ol_amount + int(row.ol_amount)
			for i in range(1,ol_count):
				ubatch.add(self.d_u_order,(d_date,w,d_id,o_id,i))

			s.execute(self.d_u_cpay,(ol_amount,w,d_id,c_id))
		s.execute(ubatch)


	def orderstatus(self, w, d, c):
		s = self.s
		
		cresults = s.execute(self.o_s_cmain,(w,d,c))
		cfirst = ""
		clast = ""
		cmiddle = ""
		for result in cresults:
			cfirst = result.c_first
			clast = result.c_last
			cmiddle = result.c_middle
		
		cresults2 = s.execute(self.o_s_cpay,(w,d,c))

		cbalance = Decimal(0)
		for result in cresults2:
			cbalance = result.c_balance
	
		results = s.execute(self.o_s_order,(w,d,c))
		
		oid = 0
		oentryd = datetime.datetime.now()
		
		for result in results:
			oid = result.o_id
			oentryd = result.o_entry_d

		results2 = s.execute(self.o_s_carr,(w,d,oid))
		
		ocarrierid = 0
		
		for result in results2:
			ocarrierid = result.o_carrier_id

		
		out = ""
		out = cfirst+","+clast+","+cmiddle+","+str(cbalance)+"\n"
		out += str(oid)+","+str(oentryd)+","+str(ocarrierid)+"\n"

		results3 = s.execute(self.o_s1_order,(w,d,oid))
		
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
			out += str(iid)+","+str(supplyid)+","+str(olquantity)+","+str(olamount)+","+str(deliveryd)+"\n"
		self.file.write(out)


	def stocklevel(self,w,d,t,l):
		s = self.s
                #query  = 'Select d_next_o_id from next_order where w_id='+str(w)+' and d_id='+str(d)
		rows = s.execute(self.s_s_next,(w,d))
		oid = 0
		for row in rows:
			oid = int(row.d_next_o_id)
		oid = oid-l
		#query = 'select ol_i_id from orderline where w_id='+str(w)+' and d_id='+str(d)+' and o_id >='+str(oid)
		rows = s.execute(self.s_s_order,(w,d,oid))
		count = 0
		itemset = set()
		for row in rows:
			item_id = int(row.ol_i_id)
			if item_id in itemset:
				continue
			#query = 'select s_quantity from item_stock where w_id ='+str(w)+' and i_id ='+str(item_id)
			result = s.execute(self.s_s_stock,(w,item_id))
			itemset.add(item_id)
			for j in result:
				if j.s_quantity <t:
					count = count+1
		out = "Total number of items in warehouse:"+str(w)+" below threshold is: "+str(count)+"\n"

                self.file.write(out)

	
	def popularItem(self,w,d,l):
		s = self.s
                rows = s.execute(self.i_s_next,(w,d))
                oid = 0
                for row in rows:
                        oid = int(row.d_next_o_id)
                oid = oid-l
                rows = s.execute(self.i_s_order,(w,d,oid))
		storerows = copy.copy(rows)
		orderdc = dict()
		for row in rows:
			item = row.ol_i_id
			oid = row.o_id
			cid = row.o_c_id
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

		
		itemdc = dict()
		out = ""
		for obj in orderdc.itervalues():
			item = obj.item
			cid = obj.cid
			result = s.execute(self.i_s_cmain,(w,d,cid))
			first = ""
			middle = ""
			last = ""
			for r in result:
				first = r.c_first
				middle = r.c_middle
				last = r.c_last
			result = s.execute(self.i_s_stock,(w,item))
			iname = ""
			for r in result:
				iname = r.i_name
			
			if  item not in itemdc:
				ob = ItemInfo(item,iname,0)
				itemdc[item] = ob
			if iname is None:
				iname = ""
			quant = 0
			if obj.quantity is not None:
				quant = obj.quantity
			out += "Order Id and Entry Date:"+str(obj.oid)+","+str(obj.entry)+"\n"
			out += "Customers who placed the order:"+first+","+middle+","+last+"\n"
			out += "Item name and quantity of popular item:"+iname+","+str(quant)+"\n"
		
		for row in storerows:
			item = row.ol_i_id
			if item in itemdc:
				obj = itemdc[item]
				obj.count = obj.count+1
		for j in itemdc.itervalues():
			name = ""
			if j.name is not None:
				name = j.name
			out += name+","+str((j.count*100)/l)+"%\n"
                self.file.write(out)

			

	def topbalance(self):
		s = self.s
		#query = 'select w_id,d_id,c_id,c_balance from c_payment'
		rows = s.execute(self.t_s_cpay)
		cust_list = []
		for row in rows:
			wid = row.w_id
			did = row.d_id
			cid = row.c_id
			bal = int(row.c_balance)
			ls = [wid,did,cid,bal]
			cust_list.append(ls)
		out = ""
		for i in sorted(cust_list,key=lambda x: x[3])[:10]:
			wid,did,cid = i[0],i[1],i[2]
			#query = 'select c_first,c_middle,c_last,w_name,d_name from customer_main where w_id = '+str(wid)+' and d_id='+str(did)+' and c_id='+str(cid)
                        r = s.execute(self.t_s_cmain,(wid,did,cid))
                        first = "vikas"
                        middle = ""
                        last = "Sherawat"
                        wname = "Warehouse"
                        dname = "district"
                        for res in r:
                                first = res.c_first
                                middle = res.c_middle
                                last = res.c_last
                        out += first+","+middle+","+last+","+str(i[3])+","+wname+","+dname+"\n"
		self.file.write(out)

