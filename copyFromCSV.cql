copy warehouse.customer_main (w_id,w_name,w_street_1,w_street_2,w_city,w_state,w_zip,w_tax,d_id,d_name,d_street_1,d_street_2,d_city,d_state,d_zip,d_tax,c_id,c_first,c_middle,c_last,c_street_1,c_street_2,c_city,c_state,c_zip,c_phone,c_since,c_credit,c_credit_lim,c_discount,c_data) from 'table1.csv' with header = true;

copy warehouse.orderline (w_id,d_id,o_id,o_c_id,o_ol_cnt,o_all_local,o_entry_d,ol_number,ol_i_id,ol_delivery_d,ol_amount,ol_supply_w_id,ol_quantity,ol_dist_info) from 'table2.csv' with header= true;

copy warehouse.item_stock (i_id,i_name,i_price,i_im_id,i_data,w_id,s_quantity,s_ytd,s_order_cnt,s_remote_cnt,s_dist_01,s_dist_02,s_dist_03,s_dist_04,s_dist_05,s_dist_06,s_dist_07,s_dist_08,s_dist_09,s_dist_10,s_data) from 'table3.csv' with header=true;

copy warehouse.o_carrier (w_id, d_id,o_id,o_carrier_id) from 'table6.csv' with header = true;


