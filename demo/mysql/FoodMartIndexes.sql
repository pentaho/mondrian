# $Id$
# This software is subject to the terms of the Common Public License
# Agreement, available at the following URL:
# http://www.opensource.org/licenses/cpl.html.
# (C) Copyright 2002-2003 Kana Software, Inc. and others.
# All Rights Reserved.
# You must accept the terms of that agreement to use this software.
#
CREATE UNIQUE INDEX i_account_id ON account ( account_id );
CREATE        INDEX i_account_parent ON account ( account_parent );
CREATE UNIQUE INDEX i_category_id ON category ( category_id );
CREATE        INDEX i_category_parent ON category ( category_parent );
CREATE UNIQUE INDEX i_currency ON currency ( currency_id, date );
CREATE        INDEX i_customer_account_num ON customer ( account_num );
CREATE        INDEX i_customer_fname ON customer ( fname );
CREATE        INDEX i_customer_lname ON customer ( lname );
CREATE        INDEX i_customer_children_at_home ON customer ( num_children_at_home );
CREATE UNIQUE INDEX i_customer_id ON customer ( customer_id );
CREATE        INDEX i_customer_postal_code ON customer ( postal_code );
CREATE        INDEX i_customer_region_id ON customer ( customer_region_id );
CREATE UNIQUE INDEX i_department_id ON department ( department_id );
CREATE UNIQUE INDEX i_employee_id ON employee ( employee_id );
CREATE        INDEX i_employee_department_id ON employee ( department_id );
CREATE        INDEX i_employee_store_id ON employee ( store_id );
CREATE        INDEX i_employee_supervisor_id ON employee ( supervisor_id );
CREATE        INDEX i_expense_store_id ON expense_fact ( store_id );
CREATE        INDEX i_expense_account_id ON expense_fact ( account_id );
CREATE        INDEX i_expense_time_id ON expense_fact ( time_id );
CREATE        INDEX i_inv_1997_product_id ON inventory_fact_1997 ( product_id );
CREATE        INDEX i_inv_1997_store_id ON inventory_fact_1997 ( store_id );
CREATE        INDEX i_inv_1997_time_id ON inventory_fact_1997 ( time_id );
CREATE        INDEX i_inv_1997_warehouse_id ON inventory_fact_1997 ( warehouse_id );
CREATE        INDEX i_inv_1998_product_id ON inventory_fact_1998 ( product_id );
CREATE        INDEX i_inv_1998_store_id ON inventory_fact_1998 ( store_id );
CREATE        INDEX i_inv_1998_time_id ON inventory_fact_1998 ( time_id );
CREATE        INDEX i_inv_1998_warehouse_id ON inventory_fact_1998 ( warehouse_id );
CREATE UNIQUE INDEX i_position_id ON position ( position_id );
CREATE        INDEX i_product_brand_name ON product ( brand_name );
CREATE UNIQUE INDEX i_product_id ON product ( product_id );
CREATE        INDEX i_product_class_id ON product ( product_class_id );
CREATE        INDEX i_product_name ON product ( product_name );
CREATE        INDEX i_product_SKU ON product ( SKU );
CREATE UNIQUE INDEX i_promotion_id ON promotion ( promotion_id );
CREATE        INDEX i_promotion_district_id ON promotion ( promotion_district_id );
CREATE UNIQUE INDEX i_reserve_employee_id ON reserve_employee ( employee_id );
CREATE        INDEX i_reserve_employee_dept_id ON reserve_employee ( department_id );
CREATE        INDEX i_reserve_employee_store_id ON reserve_employee ( store_id );
CREATE        INDEX i_reserve_employee_super_id ON reserve_employee ( supervisor_id );
CREATE        INDEX i_sales_1997_customer_id ON sales_fact_1997 ( customer_id );
CREATE        INDEX i_sales_1997_product_id ON sales_fact_1997 ( product_id );
CREATE        INDEX i_sales_1997_promotion_id ON sales_fact_1997 ( promotion_id );
CREATE        INDEX i_sales_1997_store_id ON sales_fact_1997 ( store_id );
CREATE        INDEX i_sales_1997_time_id ON sales_fact_1997 ( time_id );
CREATE        INDEX i_sales_dec_1998_customer_id ON sales_fact_dec_1998 ( customer_id );
CREATE        INDEX i_sales_dec_1998_product_id ON sales_fact_dec_1998 ( product_id );
CREATE        INDEX i_sales_dec_1998_promotion_id ON sales_fact_dec_1998 ( promotion_id );
CREATE        INDEX i_sales_dec_1998_store_id ON sales_fact_dec_1998 ( store_id );
CREATE        INDEX i_sales_dec_1998_time_id ON sales_fact_dec_1998 ( time_id );
CREATE        INDEX i_sales_1998_customer_id ON sales_fact_1998 ( customer_id );
CREATE        INDEX i_sales_1998_product_id ON sales_fact_1998 ( product_id );
CREATE        INDEX i_sales_1998_promotion_id ON sales_fact_1998 ( promotion_id );
CREATE        INDEX i_sales_1998_store_id ON sales_fact_1998 ( store_id );
CREATE        INDEX i_sales_1998_time_id ON sales_fact_1998 ( time_id );
CREATE UNIQUE INDEX i_store_id ON store ( store_id );
CREATE        INDEX i_store_region_id ON store ( region_id );
CREATE UNIQUE INDEX i_time_by_day_id ON time_by_day ( time_id );
# End FoodMartIndexes.sql
