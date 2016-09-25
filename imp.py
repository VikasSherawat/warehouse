import os
class Item:
	def __init__(self,wid,did,cid,oid,item,name):
		self.wid = wid
		self.did = did
		self.cid = cid
		self.oid = oid
		self.item = item
		self.name = name


with open('item.csv','r') as f:
	lines = [line.rstrip(os.linesep).strip('\'').split(',') for line in f]

print lines


cust = {}
for l in lines:
	pk = l[0]+l[1]+l[2]+l[3]
	dc = dict()
	oid = int(l[3])
	name = l[4]
	olid = int(l[5])
	dc["item_id"] = l[6]
	dc["item_name"] = l[7]
	dc["quantity"] = l[8]
	if pk in cust:
		obj = cust[pk]
		olines = obj.item
		olines[olid] = dc
	else:
		olines = dict()
		olines[olid] = dc
		obj = Item(l[0],l[1],l[2],l[3],olines,name)
		cust[pk] = obj	

print "printing order"

print "--------------------------------------------------------------------------------"
o=""
for j in cust.itervalues():
	o += j.wid+";"+j.did+";"+j.cid+";"+j.oid+";"+j.name+";{"	
	for oid,oitem, in j.item.iteritems():
		o += str(oid)+":"+"{"
		ids = ""
		for olid,idetails in oitem.iteritems():
			ids += olid+":"+idetails+","
		ids = ids[:-1]
		o += ids+"},"
	o = o[:-1]+"}\n"
	

f = open("orderdata.csv","w")
f.write(o)
f.close()
