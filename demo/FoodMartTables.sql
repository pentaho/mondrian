# $Id$
# This software is subject to the terms of the Common Public License
# Agreement, available at the following URL:
# http://www.opensource.org/licenses/cpl.html.
# (C) Copyright 2002 Kana Software, Inc. and others.
# All Rights Reserved.
# You must accept the terms of that agreement to use this software.
#
CREATE TABLE customer(
  customer_id INTEGER,
  ordinal INTEGER,
  account_num DOUBLE,
  lname VARCHAR,
  fname VARCHAR,
  mi VARCHAR,
  address1 VARCHAR,
  address2 VARCHAR,
  address3 VARCHAR,
  address4 VARCHAR,
  city VARCHAR,
  state_province VARCHAR,
  postal_code VARCHAR,
  country VARCHAR,
  customer_region_id INTEGER,
  phone1 VARCHAR,
  phone2 VARCHAR,
  birthdate TIMESTAMP,
  marital_status VARCHAR,
  yearly_income VARCHAR,
  gender VARCHAR,
  total_children SMALLINT,
  num_children_at_home SMALLINT,
  education VARCHAR,
  date_accnt_opened TIMESTAMP);
CREATE TABLE days(
  day INTEGER,
  week_day VARCHAR);
CREATE TABLE inventory_fact_1997(
  product_id INTEGER,
  time_id INTEGER,
  warehouse_id INTEGER,
  store_id INTEGER,
  units_ordered DOUBLE,
  units_shipped INTEGER,
  warehouse_sales NUMERIC,
  warehouse_cost NUMERIC,
  supply_time SMALLINT,
  store_invoice NUMERIC);
CREATE TABLE inventory_fact_1998(
  product_id INTEGER,
  time_id INTEGER,
  warehouse_id INTEGER,
  store_id INTEGER,
  units_ordered DOUBLE,
  units_shipped INTEGER,
  warehouse_sales NUMERIC,
  warehouse_cost NUMERIC,
  supply_time SMALLINT,
  store_invoice NUMERIC);
CREATE TABLE product(
  product_class_id INTEGER,
  product_id INTEGER,
  brand_name VARCHAR,
  product_name VARCHAR,
  SKU DOUBLE,
  SRP NUMERIC,
  gross_weight REAL,
  net_weight REAL,
  recyclable_package BIT,
  low_fat BIT,
  units_per_case SMALLINT,
  cases_per_pallet SMALLINT,
  shelf_width REAL,
  shelf_height REAL,
  shelf_depth REAL);
CREATE TABLE product_class(
  product_class_id INTEGER,
  product_subcategory VARCHAR,
  product_category VARCHAR,
  product_department VARCHAR,
  product_family VARCHAR);
CREATE TABLE promotion(
  promotion_id INTEGER,
  promotion_district_id INTEGER,
  promotion_name VARCHAR,
  media_type VARCHAR,
  cost DOUBLE,
  start_date TIMESTAMP,
  end_date TIMESTAMP);
CREATE TABLE region(
  region_id INTEGER,
  sales_city VARCHAR,
  sales_state_province VARCHAR,
  sales_district VARCHAR,
  sales_region VARCHAR,
  sales_country VARCHAR,
  sales_district_id INTEGER);
CREATE TABLE sales_fact_1997(
  product_id INTEGER,
  time_id INTEGER,
  customer_id INTEGER,
  promotion_id INTEGER,
  store_id INTEGER,
  store_sales NUMERIC,
  store_cost NUMERIC,
  unit_sales DOUBLE);
CREATE TABLE sales_fact_1998(
  product_id INTEGER,
  time_id INTEGER,
  customer_id INTEGER,
  promotion_id INTEGER,
  store_id INTEGER,
  store_sales NUMERIC,
  store_cost NUMERIC,
  unit_sales DOUBLE);
CREATE TABLE sales_fact_dec_1998(
  product_id INTEGER,
  time_id INTEGER,
  customer_id INTEGER,
  promotion_id INTEGER,
  store_id INTEGER,
  store_sales NUMERIC,
  store_cost NUMERIC,
  unit_sales DOUBLE);
CREATE TABLE store(
  store_id INTEGER,
  store_type VARCHAR,
  region_id INTEGER,
  store_name VARCHAR,
  store_number DOUBLE,
  store_street_address VARCHAR,
  store_city VARCHAR,
  store_state VARCHAR,
  store_postal_code VARCHAR,
  store_country VARCHAR,
  store_manager VARCHAR,
  store_phone VARCHAR,
  store_fax VARCHAR,
  first_opened_date TIMESTAMP,
  last_remodel_date TIMESTAMP,
  store_sqft DOUBLE,
  grocery_sqft DOUBLE,
  frozen_sqft DOUBLE,
  meat_sqft DOUBLE,
  coffee_bar BIT,
  video_store BIT,
  salad_bar BIT,
  prepared_food BIT,
  florist BIT);
CREATE TABLE time_by_day(
  time_id INTEGER,
  the_date TIMESTAMP,
  the_day VARCHAR,
  the_month VARCHAR,
  the_year SMALLINT,
  day_of_month SMALLINT,
  week_of_year DOUBLE,
  month_of_year SMALLINT,
  quarter VARCHAR,
  fiscal_period VARCHAR);
CREATE TABLE warehouse(
  warehouse_id INTEGER,
  warehouse_class_id INTEGER,
  stores_id INTEGER,
  warehouse_name VARCHAR,
  wa_address1 VARCHAR,
  wa_address2 VARCHAR,
  wa_address3 VARCHAR,
  wa_address4 VARCHAR,
  warehouse_city VARCHAR,
  warehouse_state_province VARCHAR,
  warehouse_postal_code VARCHAR,
  warehouse_country VARCHAR,
  warehouse_owner_name VARCHAR,
  warehouse_phone VARCHAR,
  warehouse_fax VARCHAR);
CREATE TABLE warehouse_class(
  warehouse_class_id INTEGER,
  description VARCHAR);
# End FoodMartTables.sql
