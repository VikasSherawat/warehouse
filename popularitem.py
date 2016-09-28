class PopularItem:
	
	def __init__(self,oid,item,quantity,entry,cid):
		self.oid = oid
		self.item = item
		self.quantity = quantity
		self.entry = entry
		self.cid = cid


class ItemInfo:

	def __init__(self,item,name,count):
		self.item = item
		self.name = name
		self.count = count
