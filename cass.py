from cassandra.cluster import Cluster

class DB:

	def getInstance(self,ip,p,db):
		#cluster = Cluster(['127.0.0.1'],port = 9042)
		cluster = Cluster(ip,port=p)
		session = cluster.connect()
		st = 'USE '+db
		session.execute(st)
		return session

	#cluster = Cluster(['127.0.0.1'],port = 9042)

