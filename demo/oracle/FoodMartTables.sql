rem $Id$
rem This software is subject to the terms of the Common Public License
rem Agreement, available at the following URL:
rem http://www.opensource.org/licenses/cpl.html.
rem (C) Copyright 2002 Kana Software, Inc. and others.
rem All Rights Reserved.
rem You must accept the terms of that agreement to use this software.
rem
rem CREATE USER foodmart IDENTIFIED BY foodmart;
rem GRANT CONNECT, RESOURCE TO foodmart;
rem CONNECT foodmart/foodmart
CREATE TABLE "customer"("customer_id" integer,"ordinal" integer,"account_num" integer,"lname" varchar(30),"fname" varchar(30),"mi" varchar(30),"address1" varchar(30),"address2" varchar(30),"address3" varchar(30),"address4" varchar(30),"city" varchar(30),"state_province" varchar(30),"postal_code" varchar(30),"country" varchar(30),"customer_region_id" integer,"phone1" varchar(30),"phone2" varchar(30),"birthdate" date,"marital_status" varchar(30),"yearly_income" varchar(30),"gender" varchar(30),"total_children" smallint,"num_children_at_home" smallint,"education" varchar(30),"date_accnt_opened" date);
CREATE TABLE "days"("day" integer,"week_day" varchar(30));
CREATE TABLE "inventory_fact_1997"("product_id" integer,"time_id" integer,"warehouse_id" integer,"store_id" integer,"units_ordered" integer,"units_shipped" integer,"warehouse_sales" numeric,"warehouse_cost" numeric,"supply_time" smallint,"store_invoice" numeric);
CREATE TABLE "inventory_fact_1998"("product_id" integer,"time_id" integer,"warehouse_id" integer,"store_id" integer,"units_ordered" integer,"units_shipped" integer,"warehouse_sales" numeric,"warehouse_cost" numeric,"supply_time" smallint,"store_invoice" numeric);
CREATE TABLE "product"("product_class_id" integer,"product_id" integer,"brand_name" varchar(60),"product_name" varchar(60),"SKU" integer,"srp" numeric,"gross_weight" real,"net_weight" real,"recyclable_package" char(1),"low_fat" char(1),"units_per_case" smallint,"cases_per_pallet" smallint,"shelf_width" real,"shelf_height" real,"shelf_depth" real);
CREATE TABLE "product_class"("product_class_id" integer,"product_subcategory" varchar(30),"product_category" varchar(30),"product_department" varchar(30),"product_family" varchar(30));
CREATE TABLE "promotion"("promotion_id" integer,"promotion_district_id" integer,"promotion_name" varchar(30),"media_type" varchar(30),"cost" real,"start_date" date,"end_date" date);
CREATE TABLE "region"("region_id" integer,"sales_city" varchar(30),"sales_state_province" varchar(30),"sales_district" varchar(30),"sales_region" varchar(30),"sales_country" varchar(30),"sales_district_id" integer);
CREATE TABLE "sales_fact_1997"("product_id" integer,"time_id" integer,"customer_id" integer,"promotion_id" integer,"store_id" integer,"store_sales" numeric,"store_cost" numeric,"unit_sales" integer);
CREATE TABLE "sales_fact_1998"("product_id" integer,"time_id" integer,"customer_id" integer,"promotion_id" integer,"store_id" integer,"store_sales" numeric,"store_cost" numeric,"unit_sales" integer);
CREATE TABLE "sales_fact_dec_1998"("product_id" integer,"time_id" integer,"customer_id" integer,"promotion_id" integer,"store_id" integer,"store_sales" numeric,"store_cost" numeric,"unit_sales" integer);
CREATE TABLE "store"("store_id" integer,"store_type" varchar(30),"region_id" integer,"store_name" varchar(30),"store_number" integer,"store_street_address" varchar(30),"store_city" varchar(30),"store_state" varchar(30),"store_postal_code" varchar(30),"store_country" varchar(30),"store_manager" varchar(30),"store_phone" varchar(30),"store_fax" varchar(30),"first_opened_date" date,"last_remodel_date" date,"store_sqft" real,"grocery_sqft" real,"frozen_sqft" real,"meat_sqft" real,"coffee_bar" char(1),"video_store" char(1),"salad_bar" char(1),"prepared_food" char(1),"florist" char(1));
CREATE TABLE "time_by_day"("time_id" integer,"the_date" date,"the_day" varchar(30),"the_month" varchar(30),"the_year" smallint,"day_of_month" smallint,"week_of_year" integer,"month_of_year" smallint,"quarter" varchar(30),"fiscal_period" varchar(30));
CREATE TABLE "warehouse"("warehouse_id" integer,"warehouse_class_id" integer,"stores_id" integer,"warehouse_name" varchar(60),"wa_address1" varchar(30),"wa_address2" varchar(30),"wa_address3" varchar(30),"wa_address4" varchar(30),"warehouse_city" varchar(30),"warehouse_state_province" varchar(30),"warehouse_postal_code" varchar(30),"warehouse_country" varchar(30),"warehouse_owner_name" varchar(30),"warehouse_phone" varchar(30),"warehouse_fax" varchar(30));
CREATE TABLE "warehouse_class"("warehouse_class_id" integer,"description" varchar(30));
rem End FoodMartTables.sql
