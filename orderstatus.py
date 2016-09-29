from cass import DB
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
import datetime
from decimal import Decimal

class OrderStatus:
	def __int__(self):
		db = DB();
		self.session = db.getInstance(['127.0.0.1'],9042,'KillrVideo')

	def orderstatus(self, w, d, c):
		s = self.session
		
		cq = 'select c_first, c_last, c_middle from customer_main where c_id = '+ str(c) +' limit 1'
		cresults = s.execute(cq)
		cfirst = ""
		clast = ""
		cmiddle = ""
		for result in cresults:
			cfirst = result.c_first
			clast = result.c_last
			cmiddle = result.c_middle
		
		cq2 = 'select c_balance from c_payment where w_id = '+ str(w) +' and d_id = '+ str(d) +' and c_id = '+ str(c) +' limit 1'
		cresults2 = s.execute(cq2)

		cbalance = 0.0
		for result in cresults2:
			cbalance = result.c_balace
	

		q = 'select o_id, o_entry_d from orderline where w_id = '+str(w)+' and d_id = '+str(d)+' and c_id = '+str(c)+' limit 1'
		results = s.execute(q)
		
		oid = 0
		oentryd = datetime.datetime.now()
		
		for result in results:
			oid = result.o_id
			oentryd = result.o_entry_d

		q2 = 'select o_carrier_id from o_carrier where o_id = '+str(oid)+' limit 1'
		results2 = s.execute(q2)
		
		ocarrierid = 0
		
		for result in results2:
			ocarrierid = result.o_carrier_id

		

		print "1. : ",cfirst,clast,cmiddle,cbalance
		print "2. : ",oid,oentryd,ocarrierid

		q3 = 'select ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d from orderline where o_id = '+str(oid)
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
