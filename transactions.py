from cass import DB
from cassandra.cluster import Cluster
import uuid
from cassandra.query import BatchStatement,SimpleStatement,BoundStatement
from popularitem import PopularItem,ItemInfo,NewOrder
import copy
import datetime
import os
from decimal import Decimal
import time

class Transactions:
	def __init__(self,fid):
		db = DB()
		self.s = db.getInstance(['127.0.0.1'],9042,'warehouse')
		self.ntime = 0.0
		self.ptime = 0.0
		self.dtime = 0.0
		self.otime = 0.0
		self.stime = 0.0
		self.Itime = 0.0
		self.Ttime = 0.0
		self.nc = 0
		self.pc = 0
		self.dc = 0
		self.oc = 0
		self.sc = 0
		self.Ic = 0
		self.Tc = 0
		#self.s = session
		self.fname = str(fid)+'-output.txt'
                if os.path.isfile(self.fname):
                        os.remove(self.fname)
		self.file = open(self.fname,'a')
				
		#queries for new order Transaction
		self.n_i_order= self.s.prepare("Insert into orderline (w_id ,d_id ,o_id ,ol_number ,o_c_id ,o_ol_cnt ,o_all_local ,o_entry_d ,ol_i_id ,ol_supply_w_id ,ol_amount, ol_quantity ,ol_dist_info,o_carrier_id,ol_i_name,ol_i_price) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
		self.n_s_next  = self.s.prepare("Select d_next_o_id from next_order where w_id=? and d_id=?")
		self.n_s_item = self.s.prepare("select i_name,i_price,s_quantity,s_ytd,s_order_cnt,s_remote_cnt from item_stock where w_id = ? and i_id =?")
		self.n_u_item = self.s.prepare("update item_stock set s_quantity = ?,s_ytd = ?,s_order_cnt = ?,s_remote_cnt = ? where w_id = ? and i_id = ?")
		self.n_u_next = self.s.prepare("update next_order set d_next_o_id = d_next_o_id + 1 where w_id=? and d_id=?")
		self.n_s_cmain =self.s.prepare("select c_last,c_credit,c_discount,w_tax,d_tax from customer_main where w_id=? and d_id=? and c_id =?") 
		self.n_i_carr = self.s.prepare("insert into o_carrier(w_id,d_id,o_id,o_carrier_id) values (?,?,?,?)")

		#queries for Payment Transaction
		self.p_u_w_pay = self.s.prepare("update w_payment set w_ytd = w_ytd + ? where w_id = ?")
		self.p_u_d_pay = self.s.prepare("update d_payment set d_ytd = d_ytd + ? where w_id = ? and d_id = ?")
		self.p_u_cpay = self.s.prepare("update c_payment set c_ytd_payment = c_ytd_payment + ?, c_payment_cnt = c_payment_cnt + 1 where c_id = ? and w_id = ? and d_id = ?")
		self.p_i_cbal = self.s.prepare("update c_balance set c_balance = c_balance + ? where w_id = ? and d_id =? and c_id = ?")

		self.p_s_cmain = self.s.prepare("select c_first, c_last, c_middle, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, w_street_1, w_street_2, w_state, w_zip, w_city, d_street_1, d_street_2, d_city, d_state, d_zip from customer_main where c_id=? and w_id=?  and d_id=?")
		#queries for Delivery Transaction
		self.d_s_carr = self.s.prepare("select o_id from o_carrier where o_carrier_id = ? and w_id=? and d_id=? limit 1")
		self.d_u_carr = self.s.prepare("update o_carrier set o_carrier_id =? where w_id =?  and d_id =? and o_id=? ")
		self.d_s_order = self.s.prepare("select o_c_id,ol_amount,o_ol_cnt from orderline where w_id=? and d_id=? and o_id =? limit 1")
		self.d_u_order = self.s.prepare("update orderline set ol_delivery_d = ? where w_id = ? and d_id = ? and o_id= ? and ol_number=?")

		#update c_balance also
		self.d_i_cbal = self.p_i_cbal

		self.d_u_cpay = self.s.prepare("update c_payment set c_delivery_cnt = c_delivery_cnt+1  where  w_id =? and d_id =? and c_id =?")

	
		#queries for OrderStatus Transaction
		self.o_s_cmain = self.s.prepare("select c_first, c_last, c_middle from customer_main where w_id=? and d_id=? and c_id=?")		
		self.o_s_cpay = self.s.prepare("select c_balance from c_balance where w_id = ? and d_id = ? and c_id = ? ")
		self.o_s_order = self.s.prepare("select o_id, o_entry_d,o_carrier_id ,ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d from orderline where w_id = ? and d_id = ? and o_c_id = ? limit 20")

		self.o_s1_order = self.s.prepare("select ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d from orderline where w_id =? and d_id =? and o_id = ?")
		
		#queries for Stock Level Transaction
		self.s_s_next = self.s.prepare("Select d_next_o_id from next_order where w_id=? and d_id=?")
		self.s_s_order = self.s.prepare("select ol_i_id from orderline where w_id=? and d_id=? and o_id >=?")
		self.s_s_stock = self.s.prepare("select s_quantity from item_stock where w_id =? and i_id =?")
		

		#queries for popular Item  Transaction
		self.i_s_next = self.s.prepare("Select d_next_o_id from next_order where w_id=? and d_id=?")
		self.i_s_order = self.s.prepare("select o_id,ol_i_id,o_entry_d,ol_quantity,o_c_id,ol_i_name from orderline where w_id=? and d_id=? and o_id >=?")
		self.i_s_cmain = self.o_s_cmain

	
		#queries for Top Balance Transaction	
		self.t_s_cpay = self.s.prepare("select w_id,d_id,c_id,c_balance from c_balance")
		self.t_s_cmain = self.s.prepare("select c_first,c_middle,c_last,w_name,d_name from customer_main where w_id = ? and d_id=? and c_id=?")

	
	def neworder(self,c,w,d,lis):
		ti = time.time()
		self.nc += 1
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
			carrier = 0
			total_amount = total_amount + item_amount
			batch.add(self.n_i_order,(w_id ,d_id ,o_id ,ol_number ,c_id ,o_ol_cnt ,o_all_local ,o_entry_d ,ol_i_id ,ol_supply_w_id,ol_amount, ol_quantity ,ol_dist_info,carrier,name,amount))
		s.execute_async(batch)
		s.execute_async(u_batch)
		s.execute_async(self.n_u_next,(w,d))
		
		
		cbatch = BatchStatement()
		cbatch.add(self.n_i_carr,(w,d,order,0))
		s.execute_async(cbatch)
		lname = ""
		credit = ""
		discount = 0.0
		wtax = 0.0
		dtax = 0.0
		future = s.execute_async(self.n_s_cmain,(w,d,c))
		try:
 			rows = future.result()
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
		except Exception:
			out = "Exception Occured"
		finally:
			self.file.write(out)
			self.ntime += time.time()-ti



	def payment(self,c,w,d,payment):
		ti = time.time()	
		self.pc += 1
		s = self.s
		
		s.execute_async(self.p_u_w_pay,(payment,w))
		s.execute_async(self.p_u_d_pay,(payment,w,d))
		s.execute_async(self.p_u_cpay,(payment,c,w,d))
		st = time.time()
		s.execute(self.d_i_cbal,(payment,w,d,c))

		future = s.execute_async(self.p_s_cmain,(c,w,d))
		
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
		try:
			results = future.result()

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

			out = str(w)+str(d)+str(c)+str(cfirst)+str(cmiddle)+str(clast)+str(cstreet1)+str(cstreet2)+str(ccity)+str(cstate)+str(czip)+str(cphone)+str(csince)+str(ccredit)+str(ccreditlim)+str(cdiscount)+str(payment)+"\n"
			out += str(wstreet1)+str(wstreet2)+str(wcity)+str(wstate)+str(wzip)+"\n"
			out += str(dstreet1)+str(dstreet2)+str(dcity)+str(dstate)+str(dzip)+"\n"
			out += str(payment)+"\n"
		except Exception:
				out = "Exception Occured"
		finally:
			self.file.write(out)
			self.ptime += time.time()-ti
			


	def delivery(self,w,carrier):
		ti = time.time()
		self.dc += 1

		s = self.s
		orderdc = {}
		for i in range(1,11):
			#query = 'select o_id from o_carrier where w_id='+str(w)+' and d_id='+str(i)+' and o_carrier_id = 1  limit 1'
			rows = s.execute(self.d_s_carr,(0,w,i))
			for row in rows:
				orderdc[i] = row.o_id
		#print "Time Taken:",time.time()-ti
		ubatch = BatchStatement()
		d_date = datetime.datetime.now()
		obatch = BatchStatement()
		ltime = time.time()
		for d_id,o_id in orderdc.iteritems():
			ubatch.add(self.d_u_carr,(carrier,w,d_id,o_id))
			future = s.execute_async(self.d_s_order,(w,d_id,o_id))
		
			try:
				rows = future.result()
				c = 0
				ol_amount = 0
				ol_count = 0
				for row in rows:
					ol_count = row.o_ol_cnt+1
					c = row.o_c_id
					ol_amount = ol_amount + int(row.ol_amount)
				for i in range(1,ol_count):
					ubatch.add(self.d_u_order,(d_date,w,d_id,o_id,i))

				s.execute_async(self.d_i_cbal,(ol_amount,w,d_id,c))
			except Exception:
				out = "Time-out in delivery"
				print out
		s.execute_async(ubatch)
		#print "Loop Time:",time.time()-ltime
		self.dtime += time.time()-ti
		
				
	def orderstatus(self, w, d, c):
		ti = time.time()
		s = self.s
		self.oc += 1
		
		cresults = s.execute(self.o_s_cmain,(w,d,c))
		fi = time.time()
		#print "1.",fi-ti
		cfirst = ""
		clast = ""
		cmiddle = ""
		for result in cresults:
			cfirst = result.c_first
			clast = result.c_last
			cmiddle = result.c_middle
		cresults2 = s.execute(self.o_s_cpay,(w,d,c))
		sq = time.time()
		#print "2.",sq-fi	
		cbalance = Decimal(0)
		for result in cresults2:
			cbalance = result.c_balance
		sq = time.time()
	
		th = time.time()
		#print "3.",th-sq
		oid = 0
		oentryd = datetime.datetime.now()
		ocarrierid = 0
		prevoid = 0
		iid = 0
		supplyid = 0
		olquantity = 0.0
		olamount = 0.0
		deliveryd = datetime.datetime.now()
		out = ""
		future = s.execute_async(self.o_s_order,(w,d,c))
		try:
			rows = future.result()
			for result in rows:
				if prevoid ==0:
					oid = result.o_id
					oentryd = result.o_entry_d
					ocarrierid = result.o_carrier_id
					out = cfirst+","+clast+","+cmiddle+","+str(cbalance)+"\n"
					out += str(oid)+","+str(oentryd)+","+str(ocarrierid)+"\n"
					
				elif result.o_id !=prevoid:
					break
				else:
					iid = result.ol_i_id
					supplyid = result.ol_supply_w_id
					olquantity = result.ol_quantity
					olamount = result.ol_amount
					deliveryd = result.ol_delivery_d
					out += str(iid)+","+str(supplyid)+","+str(olquantity)+","+str(olamount)+","+str(deliveryd)+"\n"
					

		except Exception:
			out = "Exception Occured"
			print out
		finally:			
			self.file.write(out)
			si = time.time()
			self.otime += si-ti
			

	



	def stocklevel(self,w,d,t,l):
		ti = time.time()
		self.sc += 1
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
		query = 'select s_quantity from item_stock where w_id ='+str(w)+' and i_id in ('
		
		for row in rows:
			item_id = int(row.ol_i_id)
			if item_id in itemset:
				continue
			itemset.add(item_id)
			query += str(item_id)+','
		query = query[:-1]
		query += ')'
		future = s.execute_async(query)
		try:
			result = future.result()
			for j in result:
				if j.s_quantity <t:
					count = count+1
			out = "Total number of items in warehouse:"+str(w)+" below threshold is: "+str(count)+"\n"
		except Exception:
			out = "Exception Occured"
		finally:
			self.file.write(out)
			self.stime += time.time()-ti

	

	def popularItem(self,w,d,l):
		ti = time.time()
		self.Ic += 1
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
			name = row.ol_i_name
			entry_d = row.o_entry_d
			if oid in orderdc:
				pItem = orderdc[oid]
				if pItem.quantity < quan:
					pItem.item = item
					pItem.quantity = quan
					pItem.entry = entry_d
					pItem.name = name
					orderdc[oid] = pItem

			else:
				pItem = PopularItem(oid,item,name,quan,entry_d,cid)
				orderdc[oid] = pItem	

		
		itemdc = dict()
		out = ""
		for obj in orderdc.itervalues():
			item = obj.item
			cid = obj.cid
			name = obj.name
			result = s.execute(self.i_s_cmain,(w,d,cid))
			first = ""
			middle = ""
			last = ""
			for r in result:
				first = r.c_first
				middle = r.c_middle
				last = r.c_last
			
			if  item not in itemdc:
				ob = ItemInfo(item,name,0)
				itemdc[item] = ob
			if name is None:
				name = ""
			quant = 0
			if obj.quantity is not None:
				quant = obj.quantity
			out += "Order Id and Entry Date:"+str(obj.oid)+","+str(obj.entry)+"\n"
			out += "Customers who placed the order:"+first+","+middle+","+last+"\n"
			out += "Item name and quantity of popular item:"+name+","+str(quant)+"\n"
		
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
		self.Itime += time.time()-ti


			

	def topbalance(self):
		ti = time.time()
		s = self.s
		self.Tc += 1
		query ="select w_id,d_id,c_id,c_balance from c_balance where w_id = 1 and d_id = 1"
		cust_list = []
		for i in xrange(1,11):
			for j in xrange(1,11):
				rows = s.execute(query); 
				for row in rows:
					wid = row.w_id
					did = row.d_id
					cid = row.c_id
					bal = int(row.c_balance)
					ls = [wid,did,cid,bal]
					cust_list.append(ls)
		
		for i in sorted(cust_list,key=lambda x: x[3])[-10:][::-1]:
			wid,did,cid = i[0],i[1],i[2]
			future = s.execute_async(self.t_s_cmain,(wid,did,cid))
			try:
				r = future.result()	
				first = ""
				middle = ""
				last = ""
				wname = ""
				dname = ""
				for res in r:
					first = res.c_first
					middle = res.c_middle
					last = res.c_last
					wname = res.w_name
					dname = res.d_name
				out += first+middle+last+wname+dname+"\n"
			except Exception:
				out = "Exception Occured"
			finally:
				self.file.write(out)
				self.Ttime += time.time()-ti
