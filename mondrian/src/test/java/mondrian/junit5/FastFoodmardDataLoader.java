package mondrian.junit5;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

import mondrian.spi.Dialect;
import mondrian.spi.DialectManager;

public class FastFoodmardDataLoader implements DataLoader {


    public static List<DataLoaderUtil.Table> foodmardTables = List.of(
	    new DataLoaderUtil.Table(null, "sales_fact_1997",
		    List.of(new DataLoaderUtil.Constraint("i_sls_97_cust_id", false, new String[] { "customer_id" }),
			    new DataLoaderUtil.Constraint("i_sls_97_prod_id", false, new String[] { "product_id" }),
			    new DataLoaderUtil.Constraint("i_sls_97_promo_id", false, new String[] { "promotion_id" }),
			    new DataLoaderUtil.Constraint("i_sls_97_store_id", false, new String[] { "store_id" }),
			    new DataLoaderUtil.Constraint("i_sls_97_time_id", false, new String[] { "time_id" })),
		    new DataLoaderUtil.Column("product_id", DataLoaderUtil.Type.Integer, false), new DataLoaderUtil.Column("time_id", DataLoaderUtil.Type.Integer, false),
		    new DataLoaderUtil.Column("customer_id", DataLoaderUtil.Type.Integer, false), new DataLoaderUtil.Column("promotion_id", DataLoaderUtil.Type.Integer, false),
		    new DataLoaderUtil.Column("store_id", DataLoaderUtil.Type.Integer, false), new DataLoaderUtil.Column("store_sales", DataLoaderUtil.Type.Currency, false),
		    new DataLoaderUtil.Column("store_cost", DataLoaderUtil.Type.Currency, false), new DataLoaderUtil.Column("unit_sales", DataLoaderUtil.Type.Currency, false)),
	    new DataLoaderUtil.Table(null, "sales_fact_1998",
		    List.of(new DataLoaderUtil.Constraint("i_sls_98_cust_id", false, new String[] { "customer_id" }),
			    new DataLoaderUtil.Constraint("i_sls_98_prod_id", false, new String[] { "product_id" }),
			    new DataLoaderUtil.Constraint("i_sls_98_promo_id", false, new String[] { "promotion_id" }),
			    new DataLoaderUtil.Constraint("i_sls_98_store_id", false, new String[] { "store_id" }),
			    new DataLoaderUtil.Constraint("i_sls_98_time_id", false, new String[] { "time_id" })),
		    new DataLoaderUtil.Column("product_id", DataLoaderUtil.Type.Integer, false), new DataLoaderUtil.Column("time_id", DataLoaderUtil.Type.Integer, false),
		    new DataLoaderUtil.Column("customer_id", DataLoaderUtil.Type.Integer, false), new DataLoaderUtil.Column("promotion_id", DataLoaderUtil.Type.Integer, false),
		    new DataLoaderUtil.Column("store_id", DataLoaderUtil.Type.Integer, false), new DataLoaderUtil.Column("store_sales", DataLoaderUtil.Type.Currency, false),
		    new DataLoaderUtil.Column("store_cost", DataLoaderUtil.Type.Currency, false), new DataLoaderUtil.Column("unit_sales", DataLoaderUtil.Type.Currency, false)),
	    new DataLoaderUtil.Table(null, "sales_fact_dec_1998",
		    List.of(new DataLoaderUtil.Constraint("i_sls_dec98_cust", false, new String[] { "customer_id" }),
			    new DataLoaderUtil.Constraint("i_sls_dec98_prod", false, new String[] { "product_id" }),
			    new DataLoaderUtil.Constraint("i_sls_dec98_promo", false, new String[] { "promotion_id" }), new DataLoaderUtil.Constraint(
				    "i_sls_dec98_store", false, new String[] { "store_id" }),
			    new DataLoaderUtil.Constraint("i_sls_dec98_time", false, new String[] { "time_id" })),
		    new DataLoaderUtil.Column("product_id", DataLoaderUtil.Type.Integer, false), new DataLoaderUtil.Column("time_id", DataLoaderUtil.Type.Integer, false),
		    new DataLoaderUtil.Column("customer_id", DataLoaderUtil.Type.Integer, false), new DataLoaderUtil.Column("promotion_id", DataLoaderUtil.Type.Integer, false),
		    new DataLoaderUtil.Column("store_id", DataLoaderUtil.Type.Integer, false), new DataLoaderUtil.Column("store_sales", DataLoaderUtil.Type.Currency, false),
		    new DataLoaderUtil.Column("store_cost", DataLoaderUtil.Type.Currency, false), new DataLoaderUtil.Column("unit_sales", DataLoaderUtil.Type.Currency, false)),
	    new DataLoaderUtil.Table(null, "inventory_fact_1997",
		    List.of(new DataLoaderUtil.Constraint("i_inv_97_prod_id", false, new String[] { "product_id" }),
			    new DataLoaderUtil.Constraint("i_inv_97_store_id", false, new String[] { "store_id" }),
			    new DataLoaderUtil.Constraint("i_inv_97_time_id", false, new String[] { "time_id" }),
			    new DataLoaderUtil.Constraint("i_inv_97_wrhse_id", false, new String[] { "warehouse_id" })),
		    new DataLoaderUtil.Column("product_id", DataLoaderUtil.Type.Integer, false), new DataLoaderUtil.Column("time_id", DataLoaderUtil.Type.Integer, true),
		    new DataLoaderUtil.Column("warehouse_id", DataLoaderUtil.Type.Integer, true), new DataLoaderUtil.Column("store_id", DataLoaderUtil.Type.Integer, true),
		    new DataLoaderUtil.Column("units_ordered", DataLoaderUtil.Type.Integer, true), new DataLoaderUtil.Column("units_shipped", DataLoaderUtil.Type.Integer, true),
		    new DataLoaderUtil.Column("warehouse_sales", DataLoaderUtil.Type.Currency, true),
		    new DataLoaderUtil.Column("warehouse_cost", DataLoaderUtil.Type.Currency, true), new DataLoaderUtil.Column("supply_time", DataLoaderUtil.Type.Smallint, true),
		    new DataLoaderUtil.Column("store_invoice", DataLoaderUtil.Type.Currency, true)),
	    new DataLoaderUtil.Table(null, "inventory_fact_1998",
		    List.of(new DataLoaderUtil.Constraint("i_inv_98_prod_id", false, new String[] { "product_id" }),
			    new DataLoaderUtil.Constraint("i_inv_98_store_id", false, new String[] { "store_id" }),
			    new DataLoaderUtil.Constraint("i_inv_98_time_id", false, new String[] { "time_id" }),
			    new DataLoaderUtil.Constraint("i_inv_98_wrhse_id", false, new String[] { "warehouse_id" })),
		    new DataLoaderUtil.Column("product_id", DataLoaderUtil.Type.Integer, false), new DataLoaderUtil.Column("time_id", DataLoaderUtil.Type.Integer, true),
		    new DataLoaderUtil.Column("warehouse_id", DataLoaderUtil.Type.Integer, true), new DataLoaderUtil.Column("store_id", DataLoaderUtil.Type.Integer, true),
		    new DataLoaderUtil.Column("units_ordered", DataLoaderUtil.Type.Integer, true), new DataLoaderUtil.Column("units_shipped", DataLoaderUtil.Type.Integer, true),
		    new DataLoaderUtil.Column("warehouse_sales", DataLoaderUtil.Type.Currency, true),
		    new DataLoaderUtil.Column("warehouse_cost", DataLoaderUtil.Type.Currency, true), new DataLoaderUtil.Column("supply_time", DataLoaderUtil.Type.Smallint, true),
		    new DataLoaderUtil.Column("store_invoice", DataLoaderUtil.Type.Currency, true)),

	    // Aggregate tables

	    new DataLoaderUtil.Table(null, "agg_pl_01_sales_fact_1997",
		    List.of(new DataLoaderUtil.Constraint("i_sls97pl01cust", false, new String[] { "customer_id" }),
			    new DataLoaderUtil.Constraint("i_sls97pl01prod", false, new String[] { "product_id" }),
			    new DataLoaderUtil.Constraint("i_sls97pl01time", false, new String[] { "time_id" })),
		    new DataLoaderUtil.Column("product_id", DataLoaderUtil.Type.Integer, false), new DataLoaderUtil.Column("time_id", DataLoaderUtil.Type.Integer, false),
		    new DataLoaderUtil.Column("customer_id", DataLoaderUtil.Type.Integer, false), new DataLoaderUtil.Column("store_sales_sum", DataLoaderUtil.Type.Currency, false),
		    new DataLoaderUtil.Column("store_cost_sum", DataLoaderUtil.Type.Currency, false),
		    new DataLoaderUtil.Column("unit_sales_sum", DataLoaderUtil.Type.Currency, false), new DataLoaderUtil.Column("fact_count", DataLoaderUtil.Type.Integer, false)),
	    new DataLoaderUtil.Table(null, "agg_ll_01_sales_fact_1997",
		    List.of(new DataLoaderUtil.Constraint("i_sls97ll01cust", false, new String[] { "customer_id" }),
			    new DataLoaderUtil.Constraint("i_sls97ll01prod", false, new String[] { "product_id" }),
			    new DataLoaderUtil.Constraint("i_sls97ll01time", false, new String[] { "time_id" })),
		    new DataLoaderUtil.Column("product_id", DataLoaderUtil.Type.Integer, false), new DataLoaderUtil.Column("time_id", DataLoaderUtil.Type.Integer, false),
		    new DataLoaderUtil.Column("customer_id", DataLoaderUtil.Type.Integer, false), new DataLoaderUtil.Column("store_sales", DataLoaderUtil.Type.Currency, false),
		    new DataLoaderUtil.Column("store_cost", DataLoaderUtil.Type.Currency, false), new DataLoaderUtil.Column("unit_sales", DataLoaderUtil.Type.Currency, false),
		    new DataLoaderUtil.Column("fact_count", DataLoaderUtil.Type.Integer, false)),
	    new DataLoaderUtil.Table(null, "agg_l_03_sales_fact_1997", null, new DataLoaderUtil.Column("time_id", DataLoaderUtil.Type.Integer, false),
		    new DataLoaderUtil.Column("customer_id", DataLoaderUtil.Type.Integer, false), new DataLoaderUtil.Column("store_sales", DataLoaderUtil.Type.Currency, false),
		    new DataLoaderUtil.Column("store_cost", DataLoaderUtil.Type.Currency, false), new DataLoaderUtil.Column("unit_sales", DataLoaderUtil.Type.Currency, false),
		    new DataLoaderUtil.Column("fact_count", DataLoaderUtil.Type.Integer, false)),
	    new DataLoaderUtil.Table(null, "agg_l_04_sales_fact_1997", null, new DataLoaderUtil.Column("time_id", DataLoaderUtil.Type.Integer, false),
		    new DataLoaderUtil.Column("store_sales", DataLoaderUtil.Type.Currency, false), new DataLoaderUtil.Column("store_cost", DataLoaderUtil.Type.Currency, false),
		    new DataLoaderUtil.Column("unit_sales", DataLoaderUtil.Type.Currency, false), new DataLoaderUtil.Column("customer_count", DataLoaderUtil.Type.Integer, false),
		    new DataLoaderUtil.Column("fact_count", DataLoaderUtil.Type.Integer, false)),
	    new DataLoaderUtil.Table(null, "agg_l_05_sales_fact_1997",
		    List.of(new DataLoaderUtil.Constraint("i_sls97l05cust", false, new String[] { "customer_id" }),
			    new DataLoaderUtil.Constraint("i_sls97l05prod", false, new String[] { "product_id" }),
			    new DataLoaderUtil.Constraint("i_sls97l05promo", false, new String[] { "promotion_id" }),
			    new DataLoaderUtil.Constraint("i_sls97l05store", false, new String[] { "store_id" })),
		    new DataLoaderUtil.Column("product_id", DataLoaderUtil.Type.Integer, false), new DataLoaderUtil.Column("customer_id", DataLoaderUtil.Type.Integer, false),
		    new DataLoaderUtil.Column("promotion_id", DataLoaderUtil.Type.Integer, false), new DataLoaderUtil.Column("store_id", DataLoaderUtil.Type.Integer, false),
		    new DataLoaderUtil.Column("store_sales", DataLoaderUtil.Type.Currency, false), new DataLoaderUtil.Column("store_cost", DataLoaderUtil.Type.Currency, false),
		    new DataLoaderUtil.Column("unit_sales", DataLoaderUtil.Type.Currency, false), new DataLoaderUtil.Column("fact_count", DataLoaderUtil.Type.Integer, false)),
	    new DataLoaderUtil.Table(null, "agg_c_10_sales_fact_1997", null, new DataLoaderUtil.Column("month_of_year", DataLoaderUtil.Type.Smallint, false),
		    new DataLoaderUtil.Column("quarter", DataLoaderUtil.Type.Varchar30, false), new DataLoaderUtil.Column("the_year", DataLoaderUtil.Type.Smallint, false),
		    new DataLoaderUtil.Column("store_sales", DataLoaderUtil.Type.Currency, false), new DataLoaderUtil.Column("store_cost", DataLoaderUtil.Type.Currency, false),
		    new DataLoaderUtil.Column("unit_sales", DataLoaderUtil.Type.Currency, false), new DataLoaderUtil.Column("customer_count", DataLoaderUtil.Type.Integer, false),
		    new DataLoaderUtil.Column("fact_count", DataLoaderUtil.Type.Integer, false)),
	    new DataLoaderUtil.Table(null, "agg_c_14_sales_fact_1997",
		    List.of(new DataLoaderUtil.Constraint("i_sls97c14cust", false, new String[] { "customer_id" }),
			    new DataLoaderUtil.Constraint("i_sls97c14prod", false, new String[] { "product_id" }),
			    new DataLoaderUtil.Constraint("i_sls97c14promo", false, new String[] { "promotion_id" }),
			    new DataLoaderUtil.Constraint("i_sls97c14store", false, new String[] { "store_id" })),
		    new DataLoaderUtil.Column("product_id", DataLoaderUtil.Type.Integer, false), new DataLoaderUtil.Column("customer_id", DataLoaderUtil.Type.Integer, false),
		    new DataLoaderUtil.Column("store_id", DataLoaderUtil.Type.Integer, false), new DataLoaderUtil.Column("promotion_id", DataLoaderUtil.Type.Integer, false),
		    new DataLoaderUtil.Column("month_of_year", DataLoaderUtil.Type.Smallint, false), new DataLoaderUtil.Column("quarter", DataLoaderUtil.Type.Varchar30, false),
		    new DataLoaderUtil.Column("the_year", DataLoaderUtil.Type.Smallint, false), new DataLoaderUtil.Column("store_sales", DataLoaderUtil.Type.Currency, false),
		    new DataLoaderUtil.Column("store_cost", DataLoaderUtil.Type.Currency, false), new DataLoaderUtil.Column("unit_sales", DataLoaderUtil.Type.Currency, false),
		    new DataLoaderUtil.Column("fact_count", DataLoaderUtil.Type.Integer, false)),
	    new DataLoaderUtil.Table(null, "agg_lc_100_sales_fact_1997",
		    List.of(new DataLoaderUtil.Constraint("i_sls97lc100cust", false, new String[] { "customer_id" }),
			    new DataLoaderUtil.Constraint("i_sls97lc100prod", false, new String[] { "product_id" })),
		    new DataLoaderUtil.Column("product_id", DataLoaderUtil.Type.Integer, false), new DataLoaderUtil.Column("customer_id", DataLoaderUtil.Type.Integer, false),
		    new DataLoaderUtil.Column("quarter", DataLoaderUtil.Type.Varchar30, false), new DataLoaderUtil.Column("the_year", DataLoaderUtil.Type.Smallint, false),
		    new DataLoaderUtil.Column("store_sales", DataLoaderUtil.Type.Currency, false), new DataLoaderUtil.Column("store_cost", DataLoaderUtil.Type.Currency, false),
		    new DataLoaderUtil.Column("unit_sales", DataLoaderUtil.Type.Currency, false), new DataLoaderUtil.Column("fact_count", DataLoaderUtil.Type.Integer, false)),
	    new DataLoaderUtil.Table(null, "agg_c_special_sales_fact_1997",
		    List.of(new DataLoaderUtil.Constraint("i_sls97speccust", false, new String[] { "customer_id" }),
			    new DataLoaderUtil.Constraint("i_sls97specprod", false, new String[] { "product_id" }),
			    new DataLoaderUtil.Constraint("i_sls97specpromo", false, new String[] { "promotion_id" }),
			    new DataLoaderUtil.Constraint("i_sls97specstore", false, new String[] { "store_id" })),
		    new DataLoaderUtil.Column("product_id", DataLoaderUtil.Type.Integer, false), new DataLoaderUtil.Column("promotion_id", DataLoaderUtil.Type.Integer, false),
		    new DataLoaderUtil.Column("customer_id", DataLoaderUtil.Type.Integer, false), new DataLoaderUtil.Column("store_id", DataLoaderUtil.Type.Integer, false),
		    new DataLoaderUtil.Column("time_month", DataLoaderUtil.Type.Smallint, false), new DataLoaderUtil.Column("time_quarter", DataLoaderUtil.Type.Varchar30, false),
		    new DataLoaderUtil.Column("time_year", DataLoaderUtil.Type.Smallint, false), new DataLoaderUtil.Column("store_sales_sum", DataLoaderUtil.Type.Currency, false),
		    new DataLoaderUtil.Column("store_cost_sum", DataLoaderUtil.Type.Currency, false),
		    new DataLoaderUtil.Column("unit_sales_sum", DataLoaderUtil.Type.Currency, false), new DataLoaderUtil.Column("fact_count", DataLoaderUtil.Type.Integer, false)),
	    new DataLoaderUtil.Table(null, "agg_g_ms_pcat_sales_fact_1997",
		    List.of(new DataLoaderUtil.Constraint("i_sls97gmp_gender", false, new String[] { "gender" }),
			    new DataLoaderUtil.Constraint("i_sls97gmp_ms", false, new String[] { "marital_status" }),
			    new DataLoaderUtil.Constraint("i_sls97gmp_pfam", false, new String[] { "product_family" }), new DataLoaderUtil.Constraint(
				    "i_sls97gmp_pdept", false, new String[] { "product_department" }),
			    new DataLoaderUtil.Constraint("i_sls97gmp_pcat", false, new String[] { "product_category" }),
			    new DataLoaderUtil.Constraint("i_sls97gmp_tmonth", false, new String[] { "month_of_year" }),
			    new DataLoaderUtil.Constraint("i_sls97gmp_tquarter", false, new String[] { "quarter" }),
			    new DataLoaderUtil.Constraint("i_sls97gmp_tyear", false, new String[] { "the_year" })),
		    new DataLoaderUtil.Column("gender", DataLoaderUtil.Type.Varchar30, false), new DataLoaderUtil.Column("marital_status", DataLoaderUtil.Type.Varchar30, false),
		    new DataLoaderUtil.Column("product_family", DataLoaderUtil.Type.Varchar30, true),
		    new DataLoaderUtil.Column("product_department", DataLoaderUtil.Type.Varchar30, true),
		    new DataLoaderUtil.Column("product_category", DataLoaderUtil.Type.Varchar30, true),
		    new DataLoaderUtil.Column("month_of_year", DataLoaderUtil.Type.Smallint, false), new DataLoaderUtil.Column("quarter", DataLoaderUtil.Type.Varchar30, false),
		    new DataLoaderUtil.Column("the_year", DataLoaderUtil.Type.Smallint, false), new DataLoaderUtil.Column("store_sales", DataLoaderUtil.Type.Currency, false),
		    new DataLoaderUtil.Column("store_cost", DataLoaderUtil.Type.Currency, false), new DataLoaderUtil.Column("unit_sales", DataLoaderUtil.Type.Currency, false),
		    new DataLoaderUtil.Column("customer_count", DataLoaderUtil.Type.Integer, false), new DataLoaderUtil.Column("fact_count", DataLoaderUtil.Type.Integer, false)),
	    new DataLoaderUtil.Table(null, "agg_lc_06_sales_fact_1997", null, new DataLoaderUtil.Column("time_id", DataLoaderUtil.Type.Integer, false),
		    new DataLoaderUtil.Column("city", DataLoaderUtil.Type.Varchar30, false), new DataLoaderUtil.Column("state_province", DataLoaderUtil.Type.Varchar30, false),
		    new DataLoaderUtil.Column("country", DataLoaderUtil.Type.Varchar30, false), new DataLoaderUtil.Column("store_sales", DataLoaderUtil.Type.Currency, false),
		    new DataLoaderUtil.Column("store_cost", DataLoaderUtil.Type.Currency, false), new DataLoaderUtil.Column("unit_sales", DataLoaderUtil.Type.Currency, false),
		    new DataLoaderUtil.Column("fact_count", DataLoaderUtil.Type.Integer, false)),
	    new DataLoaderUtil.Table(null, "currency",
		    List.of(new DataLoaderUtil.Constraint("i_currency", true, new String[] { "currency_id", "date" })),
		    new DataLoaderUtil.Column("currency_id", DataLoaderUtil.Type.Integer, false), new DataLoaderUtil.Column("date", DataLoaderUtil.Type.Date, false),
		    new DataLoaderUtil.Column("currency", DataLoaderUtil.Type.Varchar30, false),
		    new DataLoaderUtil.Column("conversion_ratio", DataLoaderUtil.Type.Currency, false)),
	    new DataLoaderUtil.Table(null, "account",
		    List.of(new DataLoaderUtil.Constraint("i_account_id", true, new String[] { "account_id" }),
			    new DataLoaderUtil.Constraint("i_account_parent", false, new String[] { "account_parent" })),
		    new DataLoaderUtil.Column("account_id", DataLoaderUtil.Type.Integer, false), new DataLoaderUtil.Column("account_parent", DataLoaderUtil.Type.Integer, true),
		    new DataLoaderUtil.Column("account_description", DataLoaderUtil.Type.Varchar30, true),
		    new DataLoaderUtil.Column("account_type", DataLoaderUtil.Type.Varchar30, false),
		    new DataLoaderUtil.Column("account_rollup", DataLoaderUtil.Type.Varchar30, false),
		    new DataLoaderUtil.Column("Custom_Members", DataLoaderUtil.Type.Varchar255, true)),
	    new DataLoaderUtil.Table(null, "category",
		    List.of(new DataLoaderUtil.Constraint("i_category_id", true, new String[] { "category_id" }),
			    new DataLoaderUtil.Constraint("i_category_parent", false, new String[] { "category_parent" })),
		    new DataLoaderUtil.Column("category_id", DataLoaderUtil.Type.Varchar30, false),
		    new DataLoaderUtil.Column("category_parent", DataLoaderUtil.Type.Varchar30, true),
		    new DataLoaderUtil.Column("category_description", DataLoaderUtil.Type.Varchar30, false),
		    new DataLoaderUtil.Column("category_rollup", DataLoaderUtil.Type.Varchar30, true)),
	    new DataLoaderUtil.Table(null, "customer",
		    List.of(new DataLoaderUtil.Constraint("i_cust_acct_num", false, new String[] { "account_num" }),
			    new DataLoaderUtil.Constraint("i_customer_fname", false, new String[] { "fname" }),
			    new DataLoaderUtil.Constraint("i_customer_lname", false, new String[] { "lname" }),
			    new DataLoaderUtil.Constraint("i_cust_child_home", false, new String[] { "num_children_at_home" }),
			    new DataLoaderUtil.Constraint("i_customer_id", true, new String[] { "customer_id" }),
			    new DataLoaderUtil.Constraint("i_cust_postal_code", false, new String[] { "postal_code" }),
			    new DataLoaderUtil.Constraint("i_cust_region_id", false, new String[] { "customer_region_id" })),
		    new DataLoaderUtil.Column("customer_id", DataLoaderUtil.Type.Integer, false), new DataLoaderUtil.Column("account_num", DataLoaderUtil.Type.Bigint, false),
		    new DataLoaderUtil.Column("lname", DataLoaderUtil.Type.Varchar30, false), new DataLoaderUtil.Column("fname", DataLoaderUtil.Type.Varchar30, false),
		    new DataLoaderUtil.Column("mi", DataLoaderUtil.Type.Varchar30, true), new DataLoaderUtil.Column("address1", DataLoaderUtil.Type.Varchar30, true),
		    new DataLoaderUtil.Column("address2", DataLoaderUtil.Type.Varchar30, true), new DataLoaderUtil.Column("address3", DataLoaderUtil.Type.Varchar30, true),
		    new DataLoaderUtil.Column("address4", DataLoaderUtil.Type.Varchar30, true), new DataLoaderUtil.Column("city", DataLoaderUtil.Type.Varchar30, true),
		    new DataLoaderUtil.Column("state_province", DataLoaderUtil.Type.Varchar30, true),
		    new DataLoaderUtil.Column("postal_code", DataLoaderUtil.Type.Varchar30, false), new DataLoaderUtil.Column("country", DataLoaderUtil.Type.Varchar30, false),
		    new DataLoaderUtil.Column("customer_region_id", DataLoaderUtil.Type.Integer, false), new DataLoaderUtil.Column("phone1", DataLoaderUtil.Type.Varchar30, false),
		    new DataLoaderUtil.Column("phone2", DataLoaderUtil.Type.Varchar30, false), new DataLoaderUtil.Column("birthdate", DataLoaderUtil.Type.Date, false),
		    new DataLoaderUtil.Column("marital_status", DataLoaderUtil.Type.Varchar30, false),
		    new DataLoaderUtil.Column("yearly_income", DataLoaderUtil.Type.Varchar30, false), new DataLoaderUtil.Column("gender", DataLoaderUtil.Type.Varchar30, false),
		    new DataLoaderUtil.Column("total_children", DataLoaderUtil.Type.Smallint, false),
		    new DataLoaderUtil.Column("num_children_at_home", DataLoaderUtil.Type.Smallint, false),
		    new DataLoaderUtil.Column("education", DataLoaderUtil.Type.Varchar30, false), new DataLoaderUtil.Column("date_accnt_opened", DataLoaderUtil.Type.Date, false),
		    new DataLoaderUtil.Column("member_card", DataLoaderUtil.Type.Varchar30, true), new DataLoaderUtil.Column("occupation", DataLoaderUtil.Type.Varchar30, true),
		    new DataLoaderUtil.Column("houseowner", DataLoaderUtil.Type.Varchar30, true), new DataLoaderUtil.Column("num_cars_owned", DataLoaderUtil.Type.Integer, true),
		    new DataLoaderUtil.Column("fullname", DataLoaderUtil.Type.Varchar60, false)),
	    new DataLoaderUtil.Table(null, "days", null, new DataLoaderUtil.Column("day", DataLoaderUtil.Type.Integer, false),
		    new DataLoaderUtil.Column("week_day", DataLoaderUtil.Type.Varchar30, false)),
	    new DataLoaderUtil.Table(null, "department",
		    List.of(new DataLoaderUtil.Constraint("i_department_id", true, new String[] { "department_id" })),
		    new DataLoaderUtil.Column("department_id", DataLoaderUtil.Type.Integer, false),
		    new DataLoaderUtil.Column("department_description", DataLoaderUtil.Type.Varchar30, false)),
	    new DataLoaderUtil.Table(null, "employee",
		    List.of(new DataLoaderUtil.Constraint("i_employee_id", true, new String[] { "employee_id" }),
			    new DataLoaderUtil.Constraint("i_empl_dept_id", false, new String[] { "department_id" }),
			    new DataLoaderUtil.Constraint("i_empl_store_id", false, new String[] { "store_id" }),
			    new DataLoaderUtil.Constraint("i_empl_super_id", false, new String[] { "supervisor_id" })),
		    new DataLoaderUtil.Column("employee_id", DataLoaderUtil.Type.Integer, false), new DataLoaderUtil.Column("full_name", DataLoaderUtil.Type.Varchar30, false),
		    new DataLoaderUtil.Column("first_name", DataLoaderUtil.Type.Varchar30, false), new DataLoaderUtil.Column("last_name", DataLoaderUtil.Type.Varchar30, false),
		    new DataLoaderUtil.Column("position_id", DataLoaderUtil.Type.Integer, true), new DataLoaderUtil.Column("position_title", DataLoaderUtil.Type.Varchar30, true),
		    new DataLoaderUtil.Column("store_id", DataLoaderUtil.Type.Integer, false), new DataLoaderUtil.Column("department_id", DataLoaderUtil.Type.Integer, false),
		    new DataLoaderUtil.Column("birth_date", DataLoaderUtil.Type.Date, false), new DataLoaderUtil.Column("hire_date", DataLoaderUtil.Type.Timestamp, true),
		    new DataLoaderUtil.Column("end_date", DataLoaderUtil.Type.Timestamp, true), new DataLoaderUtil.Column("salary", DataLoaderUtil.Type.Currency, false),
		    new DataLoaderUtil.Column("supervisor_id", DataLoaderUtil.Type.Integer, true),
		    new DataLoaderUtil.Column("education_level", DataLoaderUtil.Type.Varchar30, false),
		    new DataLoaderUtil.Column("marital_status", DataLoaderUtil.Type.Varchar30, false), new DataLoaderUtil.Column("gender", DataLoaderUtil.Type.Varchar30, false),
		    new DataLoaderUtil.Column("management_role", DataLoaderUtil.Type.Varchar30, true)),
	    new DataLoaderUtil.Table(null, "employee_closure",
		    List.of(new DataLoaderUtil.Constraint("i_empl_closure", true, new String[] { "supervisor_id", "employee_id" }),
			    new DataLoaderUtil.Constraint("i_empl_closure_emp", false, new String[] { "employee_id" })),
		    new DataLoaderUtil.Column("employee_id", DataLoaderUtil.Type.Integer, false), new DataLoaderUtil.Column("supervisor_id", DataLoaderUtil.Type.Integer, false),
		    new DataLoaderUtil.Column("distance", DataLoaderUtil.Type.Integer, true)),
	    new DataLoaderUtil.Table(null, "expense_fact",
		    List.of(new DataLoaderUtil.Constraint("i_expense_store_id", false, new String[] { "store_id" }),
			    new DataLoaderUtil.Constraint("i_expense_acct_id", false, new String[] { "account_id" }),
			    new DataLoaderUtil.Constraint("i_expense_time_id", false, new String[] { "time_id" })),
		    new DataLoaderUtil.Column("store_id", DataLoaderUtil.Type.Integer, false), new DataLoaderUtil.Column("account_id", DataLoaderUtil.Type.Integer, false),
		    new DataLoaderUtil.Column("exp_date", DataLoaderUtil.Type.Timestamp, false), new DataLoaderUtil.Column("time_id", DataLoaderUtil.Type.Integer, false),
		    new DataLoaderUtil.Column("category_id", DataLoaderUtil.Type.Varchar30, false), new DataLoaderUtil.Column("currency_id", DataLoaderUtil.Type.Integer, false),
		    new DataLoaderUtil.Column("amount", DataLoaderUtil.Type.Currency, false)),
	    new DataLoaderUtil.Table(null, "position", List.of(new DataLoaderUtil.Constraint("i_position_id", true, new String[] { "position_id" })),
		    new DataLoaderUtil.Column("position_id", DataLoaderUtil.Type.Integer, false), new DataLoaderUtil.Column("position_title", DataLoaderUtil.Type.Varchar30, false),
		    new DataLoaderUtil.Column("pay_type", DataLoaderUtil.Type.Varchar30, false), new DataLoaderUtil.Column("min_scale", DataLoaderUtil.Type.Currency, false),
		    new DataLoaderUtil.Column("max_scale", DataLoaderUtil.Type.Currency, false),
		    new DataLoaderUtil.Column("management_role", DataLoaderUtil.Type.Varchar30, false)),
	    new DataLoaderUtil.Table(null, "product",
		    List.of(new DataLoaderUtil.Constraint("i_prod_brand_name", false, new String[] { "brand_name" }),
			    new DataLoaderUtil.Constraint("i_product_id", true, new String[] { "product_id" }),
			    new DataLoaderUtil.Constraint("i_prod_class_id", false, new String[] { "product_class_id" }),
			    new DataLoaderUtil.Constraint("i_product_name", false, new String[] { "product_name" }),
			    new DataLoaderUtil.Constraint("i_product_SKU", false, new String[] { "SKU" })),
		    new DataLoaderUtil.Column("product_class_id", DataLoaderUtil.Type.Integer, false), new DataLoaderUtil.Column("product_id", DataLoaderUtil.Type.Integer, false),
		    new DataLoaderUtil.Column("brand_name", DataLoaderUtil.Type.Varchar60, true), new DataLoaderUtil.Column("product_name", DataLoaderUtil.Type.Varchar60, false),
		    new DataLoaderUtil.Column("SKU", DataLoaderUtil.Type.Bigint, false), new DataLoaderUtil.Column("SRP", DataLoaderUtil.Type.Currency, true),
		    new DataLoaderUtil.Column("gross_weight", DataLoaderUtil.Type.Real, true), new DataLoaderUtil.Column("net_weight", DataLoaderUtil.Type.Real, true),
		    new DataLoaderUtil.Column("recyclable_package", DataLoaderUtil.Type.Boolean, true), new DataLoaderUtil.Column("low_fat", DataLoaderUtil.Type.Boolean, true),
		    new DataLoaderUtil.Column("units_per_case", DataLoaderUtil.Type.Smallint, true),
		    new DataLoaderUtil.Column("cases_per_pallet", DataLoaderUtil.Type.Smallint, true), new DataLoaderUtil.Column("shelf_width", DataLoaderUtil.Type.Real, true),
		    new DataLoaderUtil.Column("shelf_height", DataLoaderUtil.Type.Real, true), new DataLoaderUtil.Column("shelf_depth", DataLoaderUtil.Type.Real, true)),
	    new DataLoaderUtil.Table(null, "product_class", null, new DataLoaderUtil.Column("product_class_id", DataLoaderUtil.Type.Integer, false),
		    new DataLoaderUtil.Column("product_subcategory", DataLoaderUtil.Type.Varchar30, true),
		    new DataLoaderUtil.Column("product_category", DataLoaderUtil.Type.Varchar30, true),
		    new DataLoaderUtil.Column("product_department", DataLoaderUtil.Type.Varchar30, true),
		    new DataLoaderUtil.Column("product_family", DataLoaderUtil.Type.Varchar30, true)),
	    new DataLoaderUtil.Table(null, "promotion",
		    List.of(new DataLoaderUtil.Constraint("i_promotion_id", true, new String[] { "promotion_id" }),
			    new DataLoaderUtil.Constraint("i_promo_dist_id", false, new String[] { "promotion_district_id" })),
		    new DataLoaderUtil.Column("promotion_id", DataLoaderUtil.Type.Integer, false),
		    new DataLoaderUtil.Column("promotion_district_id", DataLoaderUtil.Type.Integer, true),
		    new DataLoaderUtil.Column("promotion_name", DataLoaderUtil.Type.Varchar30, true), new DataLoaderUtil.Column("media_type", DataLoaderUtil.Type.Varchar30, true),
		    new DataLoaderUtil.Column("cost", DataLoaderUtil.Type.Currency, true), new DataLoaderUtil.Column("start_date", DataLoaderUtil.Type.Timestamp, true),
		    new DataLoaderUtil.Column("end_date", DataLoaderUtil.Type.Timestamp, true)),
	    new DataLoaderUtil.Table(null, "region", null, new DataLoaderUtil.Column("region_id", DataLoaderUtil.Type.Integer, false),
		    new DataLoaderUtil.Column("sales_city", DataLoaderUtil.Type.Varchar30, true),
		    new DataLoaderUtil.Column("sales_state_province", DataLoaderUtil.Type.Varchar30, true),
		    new DataLoaderUtil.Column("sales_district", DataLoaderUtil.Type.Varchar30, true),
		    new DataLoaderUtil.Column("sales_region", DataLoaderUtil.Type.Varchar30, true), new DataLoaderUtil.Column("sales_country", DataLoaderUtil.Type.Varchar30, true),
		    new DataLoaderUtil.Column("sales_district_id", DataLoaderUtil.Type.Integer, true)),
	    new DataLoaderUtil.Table(null, "reserve_employee",
		    List.of(new DataLoaderUtil.Constraint("i_rsrv_empl_id", true, new String[] { "employee_id" }),
			    new DataLoaderUtil.Constraint("i_rsrv_empl_dept", false, new String[] { "department_id" }),
			    new DataLoaderUtil.Constraint("i_rsrv_empl_store", false, new String[] { "store_id" }),
			    new DataLoaderUtil.Constraint("i_rsrv_empl_sup", false, new String[] { "supervisor_id" })),
		    new DataLoaderUtil.Column("employee_id", DataLoaderUtil.Type.Integer, false), new DataLoaderUtil.Column("full_name", DataLoaderUtil.Type.Varchar30, false),
		    new DataLoaderUtil.Column("first_name", DataLoaderUtil.Type.Varchar30, false), new DataLoaderUtil.Column("last_name", DataLoaderUtil.Type.Varchar30, false),
		    new DataLoaderUtil.Column("position_id", DataLoaderUtil.Type.Integer, true), new DataLoaderUtil.Column("position_title", DataLoaderUtil.Type.Varchar30, true),
		    new DataLoaderUtil.Column("store_id", DataLoaderUtil.Type.Integer, false), new DataLoaderUtil.Column("department_id", DataLoaderUtil.Type.Integer, false),
		    new DataLoaderUtil.Column("birth_date", DataLoaderUtil.Type.Timestamp, false), new DataLoaderUtil.Column("hire_date", DataLoaderUtil.Type.Timestamp, true),
		    new DataLoaderUtil.Column("end_date", DataLoaderUtil.Type.Timestamp, true), new DataLoaderUtil.Column("salary", DataLoaderUtil.Type.Currency, false),
		    new DataLoaderUtil.Column("supervisor_id", DataLoaderUtil.Type.Integer, true),
		    new DataLoaderUtil.Column("education_level", DataLoaderUtil.Type.Varchar30, false),
		    new DataLoaderUtil.Column("marital_status", DataLoaderUtil.Type.Varchar30, false), new DataLoaderUtil.Column("gender", DataLoaderUtil.Type.Varchar30, false)),
	    new DataLoaderUtil.Table(null, "salary",
		    List.of(new DataLoaderUtil.Constraint("i_salary_pay_date", false, new String[] { "pay_date" }),
			    new DataLoaderUtil.Constraint("i_salary_employee", false, new String[] { "employee_id" })),
		    new DataLoaderUtil.Column("pay_date", DataLoaderUtil.Type.Timestamp, false), new DataLoaderUtil.Column("employee_id", DataLoaderUtil.Type.Integer, false),
		    new DataLoaderUtil.Column("department_id", DataLoaderUtil.Type.Integer, false), new DataLoaderUtil.Column("currency_id", DataLoaderUtil.Type.Integer, false),
		    new DataLoaderUtil.Column("salary_paid", DataLoaderUtil.Type.Currency, false), new DataLoaderUtil.Column("overtime_paid", DataLoaderUtil.Type.Currency, false),
		    new DataLoaderUtil.Column("vacation_accrued", DataLoaderUtil.Type.Real, false), new DataLoaderUtil.Column("vacation_used", DataLoaderUtil.Type.Real, false)),
	    new DataLoaderUtil.Table(null, "store",
		    List.of(new DataLoaderUtil.Constraint("i_store_id", true, new String[] { "store_id" }),
			    new DataLoaderUtil.Constraint("i_store_region_id", false, new String[] { "region_id" })),
		    new DataLoaderUtil.Column("store_id", DataLoaderUtil.Type.Integer, false), new DataLoaderUtil.Column("store_type", DataLoaderUtil.Type.Varchar30, true),
		    new DataLoaderUtil.Column("region_id", DataLoaderUtil.Type.Integer, true), new DataLoaderUtil.Column("store_name", DataLoaderUtil.Type.Varchar30, true),
		    new DataLoaderUtil.Column("store_number", DataLoaderUtil.Type.Integer, true),
		    new DataLoaderUtil.Column("store_street_address", DataLoaderUtil.Type.Varchar30, true),
		    new DataLoaderUtil.Column("store_city", DataLoaderUtil.Type.Varchar30, true), new DataLoaderUtil.Column("store_state", DataLoaderUtil.Type.Varchar30, true),
		    new DataLoaderUtil.Column("store_postal_code", DataLoaderUtil.Type.Varchar30, true),
		    new DataLoaderUtil.Column("store_country", DataLoaderUtil.Type.Varchar30, true),
		    new DataLoaderUtil.Column("store_manager", DataLoaderUtil.Type.Varchar30, true), new DataLoaderUtil.Column("store_phone", DataLoaderUtil.Type.Varchar30, true),
		    new DataLoaderUtil.Column("store_fax", DataLoaderUtil.Type.Varchar30, true),
		    new DataLoaderUtil.Column("first_opened_date", DataLoaderUtil.Type.Timestamp, true),
		    new DataLoaderUtil.Column("last_remodel_date", DataLoaderUtil.Type.Timestamp, true), new DataLoaderUtil.Column("store_sqft", DataLoaderUtil.Type.Integer, true),
		    new DataLoaderUtil.Column("grocery_sqft", DataLoaderUtil.Type.Integer, true), new DataLoaderUtil.Column("frozen_sqft", DataLoaderUtil.Type.Integer, true),
		    new DataLoaderUtil.Column("meat_sqft", DataLoaderUtil.Type.Integer, true), new DataLoaderUtil.Column("coffee_bar", DataLoaderUtil.Type.Boolean, true),
		    new DataLoaderUtil.Column("video_store", DataLoaderUtil.Type.Boolean, true), new DataLoaderUtil.Column("salad_bar", DataLoaderUtil.Type.Boolean, true),
		    new DataLoaderUtil.Column("prepared_food", DataLoaderUtil.Type.Boolean, true), new DataLoaderUtil.Column("florist", DataLoaderUtil.Type.Boolean, true)),
	    new DataLoaderUtil.Table(null, "store_ragged",
		    List.of(new DataLoaderUtil.Constraint("i_store_raggd_id", true, new String[] { "store_id" }),
			    new DataLoaderUtil.Constraint("i_store_rggd_reg", false, new String[] { "region_id" })),
		    new DataLoaderUtil.Column("store_id", DataLoaderUtil.Type.Integer, false), new DataLoaderUtil.Column("store_type", DataLoaderUtil.Type.Varchar30, true),
		    new DataLoaderUtil.Column("region_id", DataLoaderUtil.Type.Integer, true), new DataLoaderUtil.Column("store_name", DataLoaderUtil.Type.Varchar30, true),
		    new DataLoaderUtil.Column("store_number", DataLoaderUtil.Type.Integer, true),
		    new DataLoaderUtil.Column("store_street_address", DataLoaderUtil.Type.Varchar30, true),
		    new DataLoaderUtil.Column("store_city", DataLoaderUtil.Type.Varchar30, true), new DataLoaderUtil.Column("store_state", DataLoaderUtil.Type.Varchar30, true),
		    new DataLoaderUtil.Column("store_postal_code", DataLoaderUtil.Type.Varchar30, true),
		    new DataLoaderUtil.Column("store_country", DataLoaderUtil.Type.Varchar30, true),
		    new DataLoaderUtil.Column("store_manager", DataLoaderUtil.Type.Varchar30, true), new DataLoaderUtil.Column("store_phone", DataLoaderUtil.Type.Varchar30, true),
		    new DataLoaderUtil.Column("store_fax", DataLoaderUtil.Type.Varchar30, true),
		    new DataLoaderUtil.Column("first_opened_date", DataLoaderUtil.Type.Timestamp, true),
		    new DataLoaderUtil.Column("last_remodel_date", DataLoaderUtil.Type.Timestamp, true), new DataLoaderUtil.Column("store_sqft", DataLoaderUtil.Type.Integer, true),
		    new DataLoaderUtil.Column("grocery_sqft", DataLoaderUtil.Type.Integer, true), new DataLoaderUtil.Column("frozen_sqft", DataLoaderUtil.Type.Integer, true),
		    new DataLoaderUtil.Column("meat_sqft", DataLoaderUtil.Type.Integer, true), new DataLoaderUtil.Column("coffee_bar", DataLoaderUtil.Type.Boolean, true),
		    new DataLoaderUtil.Column("video_store", DataLoaderUtil.Type.Boolean, true), new DataLoaderUtil.Column("salad_bar", DataLoaderUtil.Type.Boolean, true),
		    new DataLoaderUtil.Column("prepared_food", DataLoaderUtil.Type.Boolean, true), new DataLoaderUtil.Column("florist", DataLoaderUtil.Type.Boolean, true)),
	    new DataLoaderUtil.Table(null, "time_by_day",
		    List.of(new DataLoaderUtil.Constraint("i_time_id", true, new String[] { "time_id" }),
			    new DataLoaderUtil.Constraint("i_time_day", true, new String[] { "the_date" }),
			    new DataLoaderUtil.Constraint("i_time_year", false, new String[] { "the_year" }),
			    new DataLoaderUtil.Constraint("i_time_quarter", false, new String[] { "quarter" }),
			    new DataLoaderUtil.Constraint("i_time_month", false, new String[] { "month_of_year" })),
		    new DataLoaderUtil.Column("time_id", DataLoaderUtil.Type.Integer, false), new DataLoaderUtil.Column("the_date", DataLoaderUtil.Type.Timestamp, true),
		    new DataLoaderUtil.Column("the_day", DataLoaderUtil.Type.Varchar30, true), new DataLoaderUtil.Column("the_month", DataLoaderUtil.Type.Varchar30, true),
		    new DataLoaderUtil.Column("the_year", DataLoaderUtil.Type.Smallint, true), new DataLoaderUtil.Column("day_of_month", DataLoaderUtil.Type.Smallint, true),
		    new DataLoaderUtil.Column("week_of_year", DataLoaderUtil.Type.Integer, true), new DataLoaderUtil.Column("month_of_year", DataLoaderUtil.Type.Smallint, true),
		    new DataLoaderUtil.Column("quarter", DataLoaderUtil.Type.Varchar30, true), new DataLoaderUtil.Column("fiscal_period", DataLoaderUtil.Type.Varchar30, true)),
	    new DataLoaderUtil.Table(null, "warehouse", null, new DataLoaderUtil.Column("warehouse_id", DataLoaderUtil.Type.Integer, false),
		    new DataLoaderUtil.Column("warehouse_class_id", DataLoaderUtil.Type.Integer, true), new DataLoaderUtil.Column("stores_id", DataLoaderUtil.Type.Integer, true),
		    new DataLoaderUtil.Column("warehouse_name", DataLoaderUtil.Type.Varchar60, true), new DataLoaderUtil.Column("wa_address1", DataLoaderUtil.Type.Varchar30, true),
		    new DataLoaderUtil.Column("wa_address2", DataLoaderUtil.Type.Varchar30, true), new DataLoaderUtil.Column("wa_address3", DataLoaderUtil.Type.Varchar30, true),
		    new DataLoaderUtil.Column("wa_address4", DataLoaderUtil.Type.Varchar30, true), new DataLoaderUtil.Column("warehouse_city", DataLoaderUtil.Type.Varchar30, true),
		    new DataLoaderUtil.Column("warehouse_state_province", DataLoaderUtil.Type.Varchar30, true),
		    new DataLoaderUtil.Column("warehouse_postal_code", DataLoaderUtil.Type.Varchar30, true),
		    new DataLoaderUtil.Column("warehouse_country", DataLoaderUtil.Type.Varchar30, true),
		    new DataLoaderUtil.Column("warehouse_owner_name", DataLoaderUtil.Type.Varchar30, true),
		    new DataLoaderUtil.Column("warehouse_phone", DataLoaderUtil.Type.Varchar30, true),
		    new DataLoaderUtil.Column("warehouse_fax", DataLoaderUtil.Type.Varchar30, true)),
	    new DataLoaderUtil.Table(null, "warehouse_class", null, new DataLoaderUtil.Column("warehouse_class_id", DataLoaderUtil.Type.Integer, false),
		    new DataLoaderUtil.Column("description", DataLoaderUtil.Type.Varchar30, true)));


    @Override
    public boolean loadData(String jdbcUrl) throws Exception {
	try (Connection connection = DriverManager.getConnection(jdbcUrl);) {

	    Dialect dialect = DialectManager.createDialect(null, connection);

	    List<String> dropTableSQLs = dropTableSQLs(dialect);
	    DataLoaderUtil.executeSql(connection, dropTableSQLs);

	    List<String> createTablesSqls = createTablesSQLs(dialect);
	    DataLoaderUtil.executeSql(connection, createTablesSqls);

	    List<String> createIndexesSqls = createIndexSQLs(dialect);
	    DataLoaderUtil.executeSql(connection, createIndexesSqls);

	   Path dir= Paths.get("src/test/resources/mondrian/test/loader/data");
	    
	    DataLoaderUtil.importCSV(connection, dialect,foodmardTables,dir);

	    
	    InputStream sqlFile = getClass().getResourceAsStream("insert.sql");
	    if (sqlFile == null) {
		sqlFile = new FileInputStream(new File("src/test/resources/mondrian/test/loader/insert.sql"));
	    }
	   DataLoaderUtil.loadFromSqlInserts(connection, dialect,sqlFile);

	}
	return true;
    }
    /**
     * create indexes for the FoodMart database.
     * <p/>
     * 
     * @param dialect
     *
     */
    private List<String> createIndexSQLs(Dialect dialect) throws Exception {

	return foodmardTables.stream().flatMap(t -> DataLoaderUtil.createIndexSqls(t, dialect).stream()).toList();
    }

    /**
     * drop all existing tables for the FoodMart database.
     * <p/>
     * 
     * @param dialect
     *
     */
    private List<String> dropTableSQLs(Dialect dialect) throws Exception {

	return foodmardTables.stream().map(t -> DataLoaderUtil.dropTableSQL(t, dialect)).toList();

    }

    /**
     * Defines all tables for the FoodMart database.
     * <p/>
     * 
     * @param dialect
     *
     */
    private List<String> createTablesSQLs(Dialect dialect) throws Exception {

	return foodmardTables.stream().map(t -> DataLoaderUtil.createTableSQL(t, dialect)).toList();

    }


}
