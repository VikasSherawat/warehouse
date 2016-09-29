from cass import DB
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
import copy
import datetime
from decimal import Decimal

class Payment:
	def __init__(self):
		db = DB()
		self.session = db.getInstance(['127.0.0.1'],9042',KillrVideo')

	def payment(self,c,w,d,payment):
		s = self.session
		u_batch = BatchStatement()

		query = s.prepare("update wd_payment set w_ytd = (w_ytd + ?) where w_id = ?")
		u_batch.add(query, (payment, w))
		query2 = s.prepare("update wd_payment set d_ytd = (d_ytd + ?) where w_id = ? and d_id = ?")
		u_batch.add(query2, (payment, w, d))
		query3 = s.prepare("update c_payment set c_balance = (c_balance - ?), c_ytd_payment = (c_ytd_payment + ?), c_payment_cnt = (c_payment_cnt + 1) where c_id = ? and w_id = ? and d_id = ?")
		u_batch.add(query3, (payment, payment, c, w, d))
		s.execute(u_batch)

		q = 'select c_balance from c_payment where c_id='+str(c)+' and w_id='+str(w)+' and d_id='+str(d)+' limit 1'
		rows = s.execute(q)
		cbalance = 0.0
		for r in rows:
			cbalance = r.c_balance

		q2 = 'select c_first, c_last, c_middle, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, w_street_1, w_street_2, w_state, w_zip, w_city, d_street_1, d_street_2, d_city, d_state, d_zip from customer_main where c_id='+str(c)+' and w_id='+str(w)+' and d_id='+str(d)+' limit 1'

		results = s.execute(q2)
		
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

		print "1. ",w,d,c,cfirst,cmiddle,clast,cstreet1,cstreet2,ccity,cstate,czip,cphone,csince,ccredit,ccreditlim,cdiscount,cbalance
		print "2. ",wstreet1,wstreet2,wcity,wstate,wzip
		print "3. ",dstreet1,dstreet2,dcity,dstate,dzip
		print "4. ",payment							
