from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
from cass import DB
db = DB()
session = db.getInstance(['127.0.0.1'],9042,'warehouse')

session.execute('create table customer_main ( w_id int, d_id int, c_id int, c_first text, c_last text, c_middle text, c_street_1 text, c_street_2 text, c_city text, c_state text, c_zip text, c_phone text, c_since timestamp, c_credit text, c_credit_lim decimal, c_discount decimal, c_data text, w_name text, w_street_1 text, w_street_2 text, w_city text, w_state text, w_zip text, w_tax decimal, d_name text, d_street_1 text, d_street_2 text, d_city text, d_state text, d_zip text, d_tax decimal, PRIMARY KEY((w_id,d_id),c_id)) with clustering order by (c_id asc);')

session.execute('create table orderline ( w_id int, d_id int, o_id int, ol_number int, o_c_id int, o_ol_cnt decimal, o_all_local decimal, o_carrier_id int, o_entry_d timestamp, ol_i_id int, ol_i_name text, ol_i_price decimal, ol_supply_w_id int, ol_delivery_d timestamp, ol_amount decimal, ol_quantity decimal, ol_dist_info text, PRIMARY KEY((w_id,d_id),o_id, ol_number)) with clustering order by (o_id desc, ol_number asc);')

session.execute('create index c_id on warehouse.orderline (o_c_id);')

session.execute('create table item_stock ( w_id int, i_id int, s_quantity decimal, s_ytd decimal, s_order_cnt int, s_remote_cnt int, s_dist_01 text, s_dist_02 text, s_dist_03 text, s_dist_04 text, s_dist_05 text, s_dist_06 text, s_dist_07 text, s_dist_08 text, s_dist_09 text, s_dist_10 text, s_data text, i_name text, i_price decimal, i_im_id int, i_data text, primary key(w_id,i_id));')

session.execute('create table next_order ( w_id int, d_id int, d_next_o_id counter, primary key(w_id,d_id));')


session.execute('create table w_payment ( w_id int, w_ytd counter, primary key(w_id));')
session.execute('create table d_payment ( w_id int, d_id int, d_ytd counter, primary key((w_id,d_id)));')

session.execute('create table o_carrier ( w_id int, d_id int, o_id int, o_carrier_id int, primary key((w_id,d_id),o_id));')

session.execute('create index carrier_id on warehouse.o_carrier (o_carrier_id);')

session.execute('create table c_payment ( w_id int, d_id int, c_id int, c_ytd_payment counter, c_payment_cnt counter, c_delivery_cnt counter, primary key((w_id,d_id),c_id));')

session.execute('CREATE TABLE warehouse.c_balance (     pk int,     c_balance decimal,     stamp timestamp,     c_id int,     d_id int,     w_id int,     PRIMARY KEY (pk, c_balance, stamp) ) WITH CLUSTERING ORDER BY (c_balance ASC, stamp ASC);')

session.execute('CREATE INDEX iw_id ON warehouse.c_balance (w_id);')
session.execute('CREATE INDEX id_id ON warehouse.c_balance (d_id);')
session.execute('CREATE INDEX ic_id ON warehouse.c_balance (c_id);')

