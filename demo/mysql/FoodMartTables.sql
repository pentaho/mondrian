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
  account_description VARCHAR(30),
  account_type VARCHAR(30) NOT NULL,
  account_rollup VARCHAR(30) NOT NULL,
  Custom_Members VARCHAR(30));
CREATE TABLE category(
  category_id VARCHAR(30) NOT NULL,
  category_parent VARCHAR(30),
  category_description VARCHAR(30) NOT NULL,
  category_rollup VARCHAR(30));
CREATE TABLE currency(
  currency_id INTEGER NOT NULL,
  date TIMESTAMP NOT NULL,
  currency VARCHAR(30) NOT NULL,
  conversion_ratio DECIMAL(10,2) NOT NULL);
CREATE TABLE customer(
  customer_id INTEGER NOT NULL,
  ordinal INTEGER NOT NULL,
  account_num BIGINT NOT NULL,
  lname VARCHAR(30) NOT NULL,
  fname VARCHAR(30) NOT NULL,
  mi VARCHAR(30),
  address1 VARCHAR(30),
  address2 VARCHAR(30),
  address3 VARCHAR(30),
  address4 VARCHAR(30),
  city VARCHAR(30),
  state_province VARCHAR(30),
  postal_code VARCHAR(30) NOT NULL,
  country VARCHAR(30) NOT NULL,
  customer_region_id INTEGER NOT NULL,
  phone1 VARCHAR(30) NOT NULL,
  phone2 VARCHAR(30) NOT NULL,
  birthdate TIMESTAMP NOT NULL,
  marital_status VARCHAR(30) NOT NULL,
  yearly_income VARCHAR(30) NOT NULL,
  gender VARCHAR(30) NOT NULL,
  total_children SMALLINT NOT NULL,
  num_children_at_home SMALLINT NOT NULL,
  education VARCHAR(30) NOT NULL,
  date_accnt_opened TIMESTAMP NOT NULL,
  member_card VARCHAR(30),
  occupation VARCHAR(30),
  houseowner VARCHAR(30),
  num_cars_owned INTEGER);
CREATE TABLE days(
  day INTEGER NOT NULL,
  week_day VARCHAR(30) NOT NULL);
CREATE TABLE department(
  department_id INTEGER NOT NULL,
  department_description VARCHAR(30) NOT NULL);
CREATE TABLE employee(
  employee_id INTEGER NOT NULL,
  full_name VARCHAR(30) NOT NULL,
  first_name VARCHAR(30) NOT NULL,
  last_name VARCHAR(30) NOT NULL,
  position_id INTEGER,
  position_title VARCHAR(30),
  store_id INTEGER NOT NULL,
  department_id INTEGER NOT NULL,
  birth_date TIMESTAMP NOT NULL,
  hire_date TIMESTAMP,
  end_date TIMESTAMP,
  salary DECIMAL(10,2) NOT NULL,
  supervisor_id INTEGER,
  education_level VARCHAR(30) NOT NULL,
  marital_status VARCHAR(30) NOT NULL,
  gender VARCHAR(30) NOT NULL,
  management_role VARCHAR(30));
CREATE TABLE expense_fact(
  store_id INTEGER NOT NULL,
  account_id INTEGER NOT NULL,
  exp_date TIMESTAMP NOT NULL,
  time_id INTEGER NOT NULL,
  category_id VARCHAR(30) NOT NULL,
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
  position_title VARCHAR(30) NOT NULL,
  pay_type VARCHAR(30) NOT NULL,
  min_scale DECIMAL(10,2) NOT NULL,
  max_scale DECIMAL(10,2) NOT NULL,
  management_role VARCHAR(30) NOT NULL);
CREATE TABLE product(
  product_class_id INTEGER NOT NULL,
  product_id INTEGER NOT NULL,
  brand_name VARCHAR(30),
  product_name VARCHAR(30) NOT NULL,
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
  product_subcategory VARCHAR(30),
  product_category VARCHAR(30),
  product_department VARCHAR(30),
  product_family VARCHAR(30));
CREATE TABLE promotion(
  promotion_id INTEGER NOT NULL,
  promotion_district_id INTEGER,
  promotion_name VARCHAR(30),
  media_type VARCHAR(30),
  cost BIGINT,
  start_date TIMESTAMP,
  end_date TIMESTAMP);
CREATE TABLE region(
  region_id INTEGER NOT NULL,
  sales_city VARCHAR(30),
  sales_state_province VARCHAR(30),
  sales_district VARCHAR(30),
  sales_region VARCHAR(30),
  sales_country VARCHAR(30),
  sales_district_id INTEGER);
CREATE TABLE reserve_employee(
  employee_id INTEGER NOT NULL,
  full_name VARCHAR(30) NOT NULL,
  first_name VARCHAR(30) NOT NULL,
  last_name VARCHAR(30) NOT NULL,
  position_id INTEGER,
  position_title VARCHAR(30),
  store_id INTEGER NOT NULL,
  department_id INTEGER NOT NULL,
  birth_date TIMESTAMP NOT NULL,
  hire_date TIMESTAMP,
  end_date TIMESTAMP,
  salary DECIMAL(10,2) NOT NULL,
  supervisor_id INTEGER,
  education_level VARCHAR(30) NOT NULL,
  marital_status VARCHAR(30) NOT NULL,
  gender VARCHAR(30) NOT NULL);
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
  store_type VARCHAR(30),
  region_id INTEGER,
  store_name VARCHAR(30),
  store_number BIGINT,
  store_street_address VARCHAR(30),
  store_city VARCHAR(30),
  store_state VARCHAR(30),
  store_postal_code VARCHAR(30),
  store_country VARCHAR(30),
  store_manager VARCHAR(30),
  store_phone VARCHAR(30),
  store_fax VARCHAR(30),
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
  the_day VARCHAR(30),
  the_month VARCHAR(30),
  the_year SMALLINT,
  day_of_month SMALLINT,
  week_of_year INTEGER,
  month_of_year SMALLINT,
  quarter VARCHAR(30),
  fiscal_period VARCHAR(30));
CREATE TABLE warehouse(
  warehouse_id INTEGER NOT NULL,
  warehouse_class_id INTEGER,
  stores_id INTEGER,
  warehouse_name VARCHAR(30),
  wa_address1 VARCHAR(30),
  wa_address2 VARCHAR(30),
  wa_address3 VARCHAR(30),
  wa_address4 VARCHAR(30),
  warehouse_city VARCHAR(30),
  warehouse_state_province VARCHAR(30),
  warehouse_postal_code VARCHAR(30),
  warehouse_country VARCHAR(30),
  warehouse_owner_name VARCHAR(30),
  warehouse_phone VARCHAR(30),
  warehouse_fax VARCHAR(30));
CREATE TABLE warehouse_class(
  warehouse_class_id INTEGER NOT NULL,
  description VARCHAR(30));
# End FoodMartTables.sql
