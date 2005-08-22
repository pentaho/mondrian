##################################################################
## agg_pl_01_sales_fact_1997 done
##################################################################
# physical
# lost "promotion_id" "store_id"

INSERT INTO "agg_pl_01_sales_fact_1997" (
    "product_id", 
    "time_id",
    "customer_id",
    "store_sales_sum", 
    "store_cost_sum", 
    "unit_sales_sum", 
    "fact_count"
) SELECT
    "product_id" AS "product_id",
    "time_id" AS "time_id",
    "customer_id" AS "customer_id",
    SUM("store_sales") AS "store_sales",
    SUM("store_cost") AS "store_cost",
    SUM("unit_sales") AS "unit_sales",
    COUNT(*) AS fact_count
FROM "sales_fact_1997"
GROUP BY "product_id", "time_id", "customer_id";


INSERT INTO "agg_ll_01_sales_fact_1997" (
    "product_id", 
    "time_id",
    "customer_id",
    "store_sales", 
    "store_cost", 
    "unit_sales", 
    "fact_count"
) SELECT
    "product_id" AS "product_id",
    "time_id" AS "time_id",
    "customer_id" AS "customer_id",
    SUM("store_sales") AS "store_sales",
    SUM("store_cost") AS "store_cost",
    SUM("unit_sales") AS "unit_sales",
    COUNT(*) AS "fact_count"
FROM "sales_fact_1997"
GROUP BY "product_id", "time_id", "customer_id";

##################################################################
## agg_l_03_sales_fact_1997 done
##################################################################
# logical
# lost "product_id" "promotion_id" "store_id"


INSERT INTO "agg_l_03_sales_fact_1997" (
    "customer_id", 
    "time_id",
    "store_sales", 
    "store_cost", 
    "unit_sales", 
    "fact_count"
) SELECT
    "customer_id",
    "time_id",
    SUM("store_sales") AS "store_sales",
    SUM("store_cost") AS "store_cost",
    SUM("unit_sales") AS "unit_sales",
    COUNT(*) AS "fact_count"
FROM "sales_fact_1997"
GROUP BY "customer_id", "time_id";


##################################################################
## agg_l_05_sales_fact_1997 done
##################################################################
# logical
# lost "time_id"


INSERT INTO "agg_l_05_sales_fact_1997" (
    "product_id",
    "customer_id",
    "promotion_id",
    "store_id",
    "store_sales", 
    "store_cost", 
    "unit_sales", 
    "fact_count"
) SELECT
    "product_id",
    "customer_id",
    "promotion_id",
    "store_id",
    SUM("store_sales") AS "store_sales",
    SUM("store_cost") AS "store_cost",
    SUM("unit_sales") AS "unit_sales",
    COUNT(*) AS fact_count
FROM "sales_fact_1997"
GROUP BY "product_id", "customer_id", "promotion_id", "store_id";



##################################################################
## agg_c_14_sales_fact_1997 done
##################################################################
# collapse "time_id"


INSERT INTO "agg_c_14_sales_fact_1997" (
    "product_id", 
    "customer_id",
    "promotion_id",
    "store_id",
    "month_of_year",
    "quarter",
    "the_year",
    "store_sales", 
    "store_cost", 
    "unit_sales", 
    "fact_count"
) SELECT
    "B"."product_id",
    "B"."customer_id",
    "B"."promotion_id",
    "B"."store_id",
    "D"."month_of_year",
    "D"."quarter",
    "D"."the_year",
    SUM("B"."store_sales") AS "store_sales",
    SUM("B"."store_cost") AS "store_cost",
    SUM("B"."unit_sales") AS "unit_sales",
    COUNT(*) AS fact_count
FROM "sales_fact_1997" "B", "time_by_day" "D"
WHERE 
    "B"."time_id" = "D"."time_id"
GROUP BY "B"."product_id", 
         "B"."customer_id", 
         "B"."promotion_id",
         "B"."store_id", 
         "D"."month_of_year",
         "D"."quarter",
         "D"."the_year";


##################################################################
## agg_lc_100_sales_fact_1997 done
##################################################################
# drop "promotion_id"
# drop "store_id"
# collapse "time_id"


INSERT INTO "agg_lc_100_sales_fact_1997" (
    "product_id", 
    "customer_id",
    "quarter",
    "the_year",
    "store_sales", 
    "store_cost", 
    "unit_sales", 
    "fact_count"
) SELECT
    "B"."product_id",
    "B"."customer_id",
    "D"."quarter",
    "D"."the_year",
    SUM("B"."store_sales") AS "store_sales",
    SUM("B"."store_cost") AS "store_cost",
    SUM("B"."unit_sales") AS "unit_sales",
    COUNT(*) AS fact_count
FROM "sales_fact_1997" "B", "time_by_day" "D"
WHERE 
    "B"."time_id" = "D"."time_id"
GROUP BY "B"."product_id", 
         "B"."customer_id", 
         "D"."quarter",
         "D"."the_year";


##################################################################
##################################################################
## SPECIAL
##################################################################
##################################################################
## agg_c_special_sales_fact_1997 done
## based upon agg_c_14_sales_fact_1997
##################################################################
# collapse "time_id"


INSERT INTO "agg_c_special_sales_fact_1997" (
    "product_id", 
    "customer_id",
    "promotion_id",
    "store_id",
    "time_month",
    "time_quarter",
    "time_year",
    "store_sales_sum", 
    "store_cost_sum", 
    "unit_sales_sum", 
    "fact_count"
) SELECT
    "B"."product_id",
    "B"."customer_id",
    "B"."promotion_id",
    "B"."store_id",
    "D"."month_of_year",
    "D"."quarter",
    "D"."the_year",
    SUM("B"."store_sales") AS "store_sales_sum",
    SUM("B"."store_cost") AS "store_cost_sum",
    SUM("B"."unit_sales") AS "unit_sales_sum",
    COUNT(*) AS "fact_count"
FROM "sales_fact_1997" "B", "time_by_day" "D"
WHERE 
    "B"."time_id" = "D"."time_id"
GROUP BY "B"."product_id", 
         "B"."customer_id", 
         "B"."promotion_id",
         "B"."store_id", 
         "D"."month_of_year",
         "D"."quarter",
         "D"."the_year";
