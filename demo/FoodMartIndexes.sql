# $Id$
# This software is subject to the terms of the Common Public License
# Agreement, available at the following URL:
# http://www.opensource.org/licenses/cpl.html.
# (C) Copyright 2002 Kana Software, Inc. and others.
# All Rights Reserved.
# You must accept the terms of that agreement to use this software.
#
CREATE  INDEX i_account_num ON customer ( account_num );
CREATE  INDEX i_customer_fname ON customer ( fname );
CREATE  INDEX i_customer_lname ON customer ( lname );
CREATE  INDEX i_customer_children_at_home ON customer ( num_children_at_home );
CREATE UNIQUE INDEX i_customer_id ON customer ( customer_id );
CREATE  INDEX i_customer_postal_code ON customer ( postal_code );
CREATE  INDEX i_customer_region_id ON customer ( customer_region_id );
CREATE  INDEX i_inv_1997_product_id ON inventory_fact_1997 ( product_id );
CREATE  INDEX i_inv_1997_store_id ON inventory_fact_1997 ( store_id );
CREATE  INDEX i_inv_1997_time_id ON inventory_fact_1997 ( time_id );
CREATE  INDEX i_inv_1997_warehouse_id ON inventory_fact_1997 ( warehouse_id );
CREATE  INDEX i_inv_1998_product_id ON inventory_fact_1998 ( product_id );
CREATE  INDEX i_inv_1998_store_id ON inventory_fact_1998 ( store_id );
CREATE  INDEX i_inv_1998_time_id ON inventory_fact_1998 ( time_id );
CREATE  INDEX i_inv_1998_warehouse_id ON inventory_fact_1998 ( warehouse_id );
CREATE  INDEX i_inv_1998_brand_name ON product ( brand_name );
CREATE UNIQUE INDEX i_product_id ON product ( product_id );
CREATE  INDEX i_product_class_id ON product ( product_class_id );
CREATE  INDEX i_product_name ON product ( product_name );
CREATE  INDEX i_product_SKU ON product ( SKU );
CREATE UNIQUE INDEX i_promotion_id ON promotion ( promotion_id );
CREATE  INDEX i_promotion_district_id ON promotion ( promotion_district_id );
CREATE  INDEX i_sales_1997_customer_id ON sales_fact_1997 ( customer_id );
CREATE  INDEX i_sales_1997_product_id ON sales_fact_1997 ( product_id );
CREATE  INDEX i_sales_1997_promotion_id ON sales_fact_1997 ( promotion_id );
CREATE  INDEX i_sales_1997_store_id ON sales_fact_1997 ( store_id );
CREATE  INDEX i_sales_1997_time_id ON sales_fact_1997 ( time_id );
CREATE  INDEX i_sales_dec_1998_customer_id ON sales_fact_dec_1998 ( customer_id );
CREATE  INDEX i_sales_dec_1998_product_id ON sales_fact_dec_1998 ( product_id );
CREATE  INDEX i_sales_dec_1998_promotion_id ON sales_fact_dec_1998 ( promotion_id );
CREATE  INDEX i_sales_dec_1998_store_id ON sales_fact_dec_1998 ( store_id );
CREATE  INDEX i_sales_dec_1998_time_id ON sales_fact_dec_1998 ( time_id );
CREATE  INDEX i_sales_1998_customer_id ON sales_fact_1998 ( customer_id );
CREATE  INDEX i_sales_1998_product_id ON sales_fact_1998 ( product_id );
CREATE  INDEX i_sales_1998_promotion_id ON sales_fact_1998 ( promotion_id );
CREATE  INDEX i_sales_1998_store_id ON sales_fact_1998 ( store_id );
CREATE  INDEX i_sales_1998_time_id ON sales_fact_1998 ( time_id );
CREATE UNIQUE INDEX i_store_id ON store ( store_id );
CREATE  INDEX i_store_region_id ON store ( region_id );
# End FoodMartIndexes.sql
