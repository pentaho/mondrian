# $Id$
# This software is subject to the terms of the Common Public License
# Agreement, available at the following URL:
# http://www.opensource.org/licenses/cpl.html.
# (C) Copyright 2002-2003 Kana Software, Inc. and others.
# All Rights Reserved.
# You must accept the terms of that agreement to use this software.
#
CREATE TABLE account(
  account_id INTEGER NOT NULL,
  account_parent INTEGER,
  account_description VARCHAR,
  account_type VARCHAR NOT NULL,
  account_rollup VARCHAR NOT NULL,
  Custom_Members VARCHAR);
CREATE TABLE category(
  category_id VARCHAR NOT NULL,
  category_parent VARCHAR,
  category_description VARCHAR NOT NULL,
  category_rollup VARCHAR);
CREATE TABLE currency(
  currency_id INTEGER NOT NULL,
  date TIMESTAMP NOT NULL,
  currency VARCHAR NOT NULL,
  conversion_ratio DECIMAL(10,2) NOT NULL);
CREATE TABLE customer(
  customer_id INTEGER NOT NULL,
  ordinal INTEGER NOT NULL,
  account_num BIGINT NOT NULL,
  lname VARCHAR NOT NULL,
  fname VARCHAR NOT NULL,
  mi VARCHAR,
  address1 VARCHAR,
  address2 VARCHAR,
  address3 VARCHAR,
  address4 VARCHAR,
  city VARCHAR,
  state_province VARCHAR,
  postal_code VARCHAR NOT NULL,
  country VARCHAR NOT NULL,
  customer_region_id INTEGER NOT NULL,
  phone1 VARCHAR NOT NULL,
  phone2 VARCHAR NOT NULL,
  birthdate TIMESTAMP NOT NULL,
  marital_status VARCHAR NOT NULL,
  yearly_income VARCHAR NOT NULL,
  gender VARCHAR NOT NULL,
  total_children SMALLINT NOT NULL,
  num_children_at_home SMALLINT NOT NULL,
  education VARCHAR NOT NULL,
  date_accnt_opened TIMESTAMP NOT NULL,
  member_card VARCHAR,
  occupation VARCHAR,
  houseowner VARCHAR,
  num_cars_owned INTEGER);
CREATE TABLE days(
  day INTEGER NOT NULL,
  week_day VARCHAR NOT NULL);
CREATE TABLE department(
  department_id INTEGER NOT NULL,
  department_description VARCHAR NOT NULL);
CREATE TABLE employee(
  employee_id INTEGER NOT NULL,
  full_name VARCHAR NOT NULL,
  first_name VARCHAR NOT NULL,
  last_name VARCHAR NOT NULL,
  position_id INTEGER,
  position_title VARCHAR,
  store_id INTEGER NOT NULL,
  department_id INTEGER NOT NULL,
  birth_date TIMESTAMP NOT NULL,
  hire_date TIMESTAMP,
  end_date TIMESTAMP,
  salary DECIMAL(10,2) NOT NULL,
  supervisor_id INTEGER,
  education_level VARCHAR NOT NULL,
  marital_status VARCHAR NOT NULL,
  gender VARCHAR NOT NULL,
  management_role VARCHAR);
CREATE TABLE employee_closure(
  employee_id INTEGER NOT NULL,
  supervisor_id INTEGER NOT NULL,
  distance INTEGER);
CREATE TABLE expense_fact(
  store_id INTEGER NOT NULL,
  account_id INTEGER NOT NULL,
  exp_date TIMESTAMP NOT NULL,
  time_id INTEGER NOT NULL,
  category_id VARCHAR NOT NULL,
  currency_id INTEGER NOT NULL,
  amount DECIMAL(10,2) NOT NULL);
CREATE TABLE inventory_fact_1997(
  product_id INTEGER NOT NULL,
  time_id INTEGER,
  warehouse_id INTEGER,
  store_id INTEGER,
  units_ordered INTEGER,
  units_shipped INTEGER,
  warehouse_sales DECIMAL(10,2),
  warehouse_cost DECIMAL(10,2),
  supply_time SMALLINT,
  store_invoice DECIMAL(10,2));
CREATE TABLE inventory_fact_1998(
  product_id INTEGER NOT NULL,
  time_id INTEGER,
  warehouse_id INTEGER,
  store_id INTEGER,
  units_ordered INTEGER,
  units_shipped INTEGER,
  warehouse_sales DECIMAL(10,2),
  warehouse_cost DECIMAL(10,2),
  supply_time SMALLINT,
  store_invoice DECIMAL(10,2));
CREATE TABLE position(
  position_id INTEGER NOT NULL,
  position_title VARCHAR NOT NULL,
  pay_type VARCHAR NOT NULL,
  min_scale DECIMAL(10,2) NOT NULL,
  max_scale DECIMAL(10,2) NOT NULL,
  management_role VARCHAR NOT NULL);
CREATE TABLE product(
  product_class_id INTEGER NOT NULL,
  product_id INTEGER NOT NULL,
  brand_name VARCHAR,
  product_name VARCHAR NOT NULL,
  SKU BIGINT NOT NULL,
  SRP DECIMAL(10,2),
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
  product_class_id INTEGER NOT NULL,
  product_subcategory VARCHAR,
  product_category VARCHAR,
  product_department VARCHAR,
  product_family VARCHAR);
CREATE TABLE promotion(
  promotion_id INTEGER NOT NULL,
  promotion_district_id INTEGER,
  promotion_name VARCHAR,
  media_type VARCHAR,
  cost BIGINT,
  start_date TIMESTAMP,
  end_date TIMESTAMP);
CREATE TABLE region(
  region_id INTEGER NOT NULL,
  sales_city VARCHAR,
  sales_state_province VARCHAR,
  sales_district VARCHAR,
  sales_region VARCHAR,
  sales_country VARCHAR,
  sales_district_id INTEGER);
CREATE TABLE reserve_employee(
  employee_id INTEGER NOT NULL,
  full_name VARCHAR NOT NULL,
  first_name VARCHAR NOT NULL,
  last_name VARCHAR NOT NULL,
  position_id INTEGER,
  position_title VARCHAR,
  store_id INTEGER NOT NULL,
  department_id INTEGER NOT NULL,
  birth_date TIMESTAMP NOT NULL,
  hire_date TIMESTAMP,
  end_date TIMESTAMP,
  salary DECIMAL(10,2) NOT NULL,
  supervisor_id INTEGER,
  education_level VARCHAR NOT NULL,
  marital_status VARCHAR NOT NULL,
  gender VARCHAR NOT NULL);
CREATE TABLE salary(
  pay_date TIMESTAMP NOT NULL,
  employee_id INTEGER NOT NULL,
  department_id INTEGER NOT NULL,
  currency_id INTEGER NOT NULL,
  salary_paid DECIMAL(10,2) NOT NULL,
  overtime_paid DECIMAL(10,2) NOT NULL,
  vacation_accrued INTEGER NOT NULL,
  vacation_used INTEGER NOT NULL);
CREATE TABLE sales_fact_1997(
  product_id INTEGER NOT NULL,
  time_id INTEGER NOT NULL,
  customer_id INTEGER NOT NULL,
  promotion_id INTEGER NOT NULL,
  store_id INTEGER NOT NULL,
  store_sales DECIMAL(10,2) NOT NULL,
  store_cost DECIMAL(10,2) NOT NULL,
  unit_sales BIGINT NOT NULL);
CREATE TABLE sales_fact_1998(
  product_id INTEGER NOT NULL,
  time_id INTEGER NOT NULL,
  customer_id INTEGER NOT NULL,
  promotion_id INTEGER NOT NULL,
  store_id INTEGER NOT NULL,
  store_sales DECIMAL(10,2) NOT NULL,
  store_cost DECIMAL(10,2) NOT NULL,
  unit_sales BIGINT NOT NULL);
CREATE TABLE sales_fact_dec_1998(
  product_id INTEGER NOT NULL,
  time_id INTEGER NOT NULL,
  customer_id INTEGER NOT NULL,
  promotion_id INTEGER NOT NULL,
  store_id INTEGER NOT NULL,
  store_sales DECIMAL(10,2) NOT NULL,
  store_cost DECIMAL(10,2) NOT NULL,
  unit_sales BIGINT NOT NULL);
CREATE TABLE store(
  store_id INTEGER NOT NULL,
  store_type VARCHAR,
  region_id INTEGER,
  store_name VARCHAR,
  store_number BIGINT,
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
  store_sqft BIGINT,
  grocery_sqft BIGINT,
  frozen_sqft BIGINT,
  meat_sqft BIGINT,
  coffee_bar BIT,
  video_store BIT,
  salad_bar BIT,
  prepared_food BIT,
  florist BIT);
CREATE TABLE time_by_day(
  time_id INTEGER NOT NULL,
  the_date TIMESTAMP,
  the_day VARCHAR,
  the_month VARCHAR,
  the_year SMALLINT,
  day_of_month SMALLINT,
  week_of_year INTEGER,
  month_of_year SMALLINT,
  quarter VARCHAR,
  fiscal_period VARCHAR);
CREATE TABLE warehouse(
  warehouse_id INTEGER NOT NULL,
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
  warehouse_class_id INTEGER NOT NULL,
  description VARCHAR);
# End FoodMartTables.sql
