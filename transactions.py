from cass import DB
from cassandra.cluster import Cluster

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
		query = 'select item_line from most_popular_item where w_id='+str(w)+' and d_id='+str(d)+' limit '+str(l)
                rows = s.execute(query)
                item = set()
                for row in rows:
			item.add(row.item_line["item_name"])
		
		query = 'select item_id,s_quantity from stock_level where w_id='+str(w)+' and d_id='+str(d)+' and item_id in '+set
		s.execute(query)
		
		count = 0
		for row in rows:
			if int(row.s_quantity) < t:
				count += 1
		

	def popularItem(self,w,d,l):
		s = self.session
		query = 'select item_line from most_popular_item where w_id='+str(w)+' and d_id='+str(d)+' limit '+str(l)
		rows = s.execute(query)
		item = dict()
		for row in rows:
			if row.item_line["item_name"] in item:
				item["item_name"] +=row.item_line["quantity"]
			else:
				item["item_name"] = row.item_line["quantity"]
		print max(item.iteritems(), key=operator.itemgetter(1))[0]


	def topbalance(self):
		s = self.session
		query = 'select C_FIRST_NAME,C_MIDDLE_NAME,C_LAST_NAME,C_BALANCE,W_NAME,D_NAME from top_balance limit 10'
		rows = s.execute(query)
		for row in rows:
			name = row.C_FIRST_NAME+" "+row.C_MIDDLE_NAME+" "+row.C_LAST_NAME
			balance = row.C_BALANCE
			wname = row.W_NAME
			dname = row.D_NAME
