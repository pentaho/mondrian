rem $Id$
rem This software is subject to the terms of the Common Public License
rem Agreement, available at the following URL:
rem http://www.opensource.org/licenses/cpl.html.
rem (C) Copyright 2002 Kana Software, Inc. and others.
rem All Rights Reserved.
rem You must accept the terms of that agreement to use this software.
rem
rem Before running this script log in as SYSTEM, and do the following:
rem   CREATE USER foodmart IDENTIFIED BY foodmart;
rem   GRANT CONNECT, RESOURCE TO foodmart;
rem   CONNECT foodmart/foodmart

CREATE TABLE "customer"(
  "customer_id" INTEGER NOT NULL,
  "ordinal" INTEGER NOT NULL,
  "account_num" INTEGER NOT NULL,
  "lname" VARCHAR(30) NOT NULL,
  "fname" VARCHAR(30) NOT NULL,
  "mi" VARCHAR(30),
  "address1" VARCHAR(30),
  "address2" VARCHAR(30),
  "address3" VARCHAR(30),
  "address4" VARCHAR(30),
  "city" VARCHAR(30),
  "state_province" VARCHAR(30),
  "postal_code" VARCHAR(30) NOT NULL,
  "country" VARCHAR(30) NOT NULL,
  "customer_region_id" INTEGER NOT NULL,
  "phone1" VARCHAR(30) NOT NULL,
  "phone2" VARCHAR(30) NOT NULL,
  "birthdate" DATE NOT NULL,
  "marital_status" VARCHAR(30) NOT NULL,
  "yearly_income" VARCHAR(30) NOT NULL,
  "gender" VARCHAR(30) NOT NULL,
  "total_children" SMALLINT NOT NULL,
  "num_children_at_home" SMALLINT NOT NULL,
  "education" VARCHAR(30) NOT NULL,
  "date_accnt_opened" DATE NOT NULL);
CREATE TABLE "days"(
  "day" INTEGER,
  "week_day" VARCHAR(30));
CREATE TABLE "inventory_fact_1997"(
  "product_id" INTEGER NOT NULL,
  "time_id" INTEGER,
  "warehouse_id" INTEGER,
  "store_id" INTEGER,
  "units_ordered" INTEGER,
  "units_shipped" INTEGER,
  "warehouse_sales" DECIMAL(10,4),
  "warehouse_cost" DECIMAL(10,4),
  "supply_time" SMALLINT,
  "store_invoice" DECIMAL(10,4));
CREATE TABLE "inventory_fact_1998"(
  "product_id" INTEGER NOT NULL,
  "time_id" INTEGER,
  "warehouse_id" INTEGER,
  "store_id" INTEGER,
  "units_ordered" INTEGER,
  "units_shipped" INTEGER,
  "warehouse_sales" DECIMAL(10,4),
  "warehouse_cost" DECIMAL(10,4),
  "supply_time" SMALLINT,
  "store_invoice" DECIMAL(10,4));
CREATE TABLE "product"(
  "product_class_id" INTEGER NOT NULL,
  "product_id" INTEGER NOT NULL,
  "brand_name" VARCHAR(60),
  "product_name" VARCHAR(60) NOT NULL,
  "SKU" INTEGER NOT NULL,
  "SRP" DECIMAL(10,2),
  "gross_weight" REAL,
  "net_weight" REAL,
  "recyclable_package" CHAR(1),
  "low_fat" CHAR(1),
  "units_per_case" SMALLINT,
  "cases_per_pallet" SMALLINT,
  "shelf_width" REAL,
  "shelf_height" REAL,
  "shelf_depth" REAL);
CREATE TABLE "product_class"(
  "product_class_id" INTEGER NOT NULL,
  "product_subcategory" VARCHAR(30),
  "product_category" VARCHAR(30),
  "product_department" VARCHAR(30),
  "product_family" VARCHAR(30));
CREATE TABLE "promotion"(
  "promotion_id" INTEGER NOT NULL,
  "promotion_district_id" INTEGER,
  "promotion_name" VARCHAR(30),
  "media_type" VARCHAR(30),
  "cost" REAL,
  "start_date" DATE,
  "end_date" DATE);
CREATE TABLE "region"(
  "region_id" INTEGER NOT NULL,
  "sales_city" VARCHAR(30),
  "sales_state_province" VARCHAR(30),
  "sales_district" VARCHAR(30),
  "sales_region" VARCHAR(30),
  "sales_country" VARCHAR(30),
  "sales_district_id" INTEGER);
CREATE TABLE "sales_fact_1997"(
  "product_id" INTEGER NOT NULL,
  "time_id" INTEGER NOT NULL,
  "customer_id" INTEGER NOT NULL,
  "promotion_id" INTEGER NOT NULL,
  "store_id" INTEGER NOT NULL,
  "store_sales" DECIMAL(10,2) NOT NULL,
  "store_cost" DECIMAL(10,4) NOT NULL,
  "unit_sales" INTEGER NOT NULL);
CREATE TABLE "sales_fact_1998"(
  "product_id" INTEGER NOT NULL,
  "time_id" INTEGER NOT NULL,
  "customer_id" INTEGER NOT NULL,
  "promotion_id" INTEGER NOT NULL,
  "store_id" INTEGER NOT NULL,
  "store_sales" DECIMAL(10,2) NOT NULL,
  "store_cost" DECIMAL(10,4) NOT NULL,
  "unit_sales" INTEGER NOT NULL);
CREATE TABLE "sales_fact_dec_1998"(
  "product_id" INTEGER NOT NULL,
  "time_id" INTEGER NOT NULL,
  "customer_id" INTEGER NOT NULL,
  "promotion_id" INTEGER NOT NULL,
  "store_id" INTEGER NOT NULL,
  "store_sales" DECIMAL(10,2) NOT NULL,
  "store_cost" DECIMAL(10,4) NOT NULL,
  "unit_sales" INTEGER NOT NULL);
CREATE TABLE "store"(
  "store_id" INTEGER NOT NULL,
  "store_type" VARCHAR(30),
  "region_id" INTEGER,
  "store_name" VARCHAR(30),
  "store_number" INTEGER,
  "store_street_address" VARCHAR(30),
  "store_city" VARCHAR(30),
  "store_state" VARCHAR(30),
  "store_postal_code" VARCHAR(30),
  "store_country" VARCHAR(30),
  "store_manager" VARCHAR(30),
  "store_phone" VARCHAR(30),
  "store_fax" VARCHAR(30),
  "first_opened_date" DATE,
  "last_remodel_date" DATE,
  "store_sqft" REAL,
  "grocery_sqft" REAL,
  "frozen_sqft" REAL,
  "meat_sqft" REAL,
  "coffee_bar" CHAR(1),
  "video_store" CHAR(1),
  "salad_bar" CHAR(1),
  "prepared_food" CHAR(1),
  "florist" CHAR(1));
CREATE TABLE "time_by_day"(
  "time_id" INTEGER NOT NULL,
  "the_date" DATE,
  "the_day" VARCHAR(30),
  "the_month" VARCHAR(30),
  "the_year" SMALLINT,
  "day_of_month" SMALLINT,
  "week_of_year" INTEGER,
  "month_of_year" SMALLINT,
  "quarter" VARCHAR(30),
  "fiscal_period" VARCHAR(30));
CREATE TABLE "warehouse"(
  "warehouse_id" INTEGER NOT NULL,
  "warehouse_class_id" INTEGER,
  "stores_id" INTEGER,
  "warehouse_name" VARCHAR(60),
  "wa_address1" VARCHAR(30),
  "wa_address2" VARCHAR(30),
  "wa_address3" VARCHAR(30),
  "wa_address4" VARCHAR(30),
  "warehouse_city" VARCHAR(30),
  "warehouse_state_province" VARCHAR(30),
  "warehouse_postal_code" VARCHAR(30),
  "warehouse_country" VARCHAR(30),
  "warehouse_owner_name" VARCHAR(30),
  "warehouse_phone" VARCHAR(30),
  "warehouse_fax" VARCHAR(30));
CREATE TABLE "warehouse_class"(
  "warehouse_class_id" INTEGER NOT NULL,
  "description" VARCHAR(30));
rem End FoodMartTables.sql
