# This software is subject to the terms of the Eclipse Public License v1.0
# Agreement, available at the following URL:
# http://www.eclipse.org/legal/epl-v10.html.
# You must accept the terms of that agreement to use this software.
#
# Copyright (C) 2004-2005 Julian Hyde
# Copyright (C) 2005-2007 Pentaho
# All Rights Reserved.

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
## agg_lc_06_sales_fact_1997 done
##################################################################
# collapse "customer_id"
# lost "product_id" "promotion_id" "store_id"


INSERT INTO "agg_lc_06_sales_fact_1997" (
    "time_id",
    "city",
    "state_province",
    "country",
    "store_sales", 
    "store_cost", 
    "unit_sales", 
    "fact_count"
) SELECT
    "B"."time_id",
    "D"."city",
    "D"."state_province",
    "D"."country",
    SUM("B"."store_sales") AS "store_sales",
    SUM("B"."store_cost") AS "store_cost",
    SUM("B"."unit_sales") AS "unit_sales",
    COUNT(*) AS fact_count
FROM "sales_fact_1997" "B", "customer" "D"
WHERE 
    "B"."customer_id" = "D"."customer_id"
GROUP BY 
         "B"."time_id",
         "D"."city",
         "D"."state_province",
         "D"."country";

##################################################################
## agg_l_04_sales_fact_1997 done
##################################################################
# logical
# lost "customer_id" "product_id" "promotion_id" "store_id"

INSERT INTO "agg_l_04_sales_fact_1997" (
    "time_id",
    "store_sales", 
    "store_cost", 
    "unit_sales", 
    "customer_count", 
    "fact_count"
) SELECT
    "time_id",
    SUM("store_sales") AS "store_sales",
    SUM("store_cost") AS "store_cost",
    SUM("unit_sales") AS "unit_sales",
    COUNT(DISTINCT "customer_id") AS "customer_count",
    COUNT(*) AS "fact_count"
FROM "sales_fact_1997"
GROUP BY "time_id";

##################################################################
## agg_c_10_sales_fact_1997 done
##################################################################
# collapse "time_id"
# lost "customer_id" "product_id" "promotion_id" "store_id"


INSERT INTO "agg_c_10_sales_fact_1997" (
    "month_of_year",
    "quarter",
    "the_year",
    "store_sales", 
    "store_cost", 
    "unit_sales", 
    "customer_count", 
    "fact_count"
) SELECT
    "D"."month_of_year",
    "D"."quarter",
    "D"."the_year",
    SUM("B"."store_sales") AS "store_sales",
    SUM("B"."store_cost") AS "store_cost",
    SUM("B"."unit_sales") AS "unit_sales",
    COUNT(DISTINCT "customer_id") AS "customer_count",
    COUNT(*) AS fact_count
FROM "sales_fact_1997" "B", "time_by_day" "D"
WHERE 
    "B"."time_id" = "D"."time_id"
GROUP BY 
         "D"."month_of_year",
         "D"."quarter",
         "D"."the_year";

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

##################################################################
# agg_gender_ms_state_sales_fact_1997
##################################################################

INSERT INTO "agg_g_ms_pcat_sales_fact_1997" (
    "gender", 
    "marital_status",
    "product_family",
    "product_department",
    "product_category",
    "month_of_year",
    "quarter",
    "the_year",
    "store_sales", 
    "store_cost", 
    "unit_sales", 
    "customer_count", 
    "fact_count"
) SELECT
    "C"."gender",
    "C"."marital_status",
    "PC"."product_family",
    "PC"."product_department",
    "PC"."product_category",
    "T"."month_of_year",
    "T"."quarter",
    "T"."the_year",
    SUM("B"."store_sales") AS "store_sales",
    SUM("B"."store_cost") AS "store_cost",
    SUM("B"."unit_sales") AS "unit_sales",
    COUNT(DISTINCT "C"."customer_id") AS "customer_count",
    COUNT(*) AS "fact_count"
FROM "sales_fact_1997" "B",
    "time_by_day" "T",
    "product" "P",
    "product_class" "PC",
    "customer" "C"
WHERE 
    "B"."time_id" = "T"."time_id"
AND "B"."customer_id" = "C"."customer_id"
AND "B"."product_id" = "P"."product_id"
AND "P"."product_class_id" = "PC"."product_class_id"
GROUP BY 
    "C"."gender",
    "C"."marital_status",
    "PC"."product_family",
    "PC"."product_department",
    "PC"."product_category",
    "T"."month_of_year",
    "T"."quarter",
    "T"."the_year";

# Above query, rephrased for Access (which does not support
# COUNT(DISTINCT ...) explicitly.
#
#INSERT INTO "agg_g_ms_pcat_sales_fact_1997" (
#    "gender", 
#    "marital_status",
#    "product_family",
#    "product_department",
#    "product_category",
#    "month_of_year",
#    "quarter",
#    "the_year",
#    "store_sales", 
#    "store_cost", 
#    "unit_sales", 
#    "customer_count", 
#    "fact_count"
#) SELECT
#    "C"."gender",
#    "C"."marital_status",
#    "PC"."product_family",
#    "PC"."product_department",
#    "PC"."product_category",
#    "T"."month_of_year",
#    "T"."quarter",
#    "T"."the_year",
#    SUM("B"."store_sales") AS "store_sales",
#    SUM("B"."store_cost") AS "store_cost",
#    SUM("B"."unit_sales") AS "unit_sales",
#    (
#    SELECT COUNT("customer_id")
#    FROM (
#        SELECT DISTINCT
#            "DC"."gender",
#            "DC"."marital_status",
#            "DPC"."product_family",
#            "DPC"."product_department",
#            "DPC"."product_category",
#            "DT"."month_of_year",
#            "DT"."quarter",
#            "DT"."the_year",
#            "DB"."customer_id"
#        FROM
#            "sales_fact_1997" "DB",
#            "time_by_day" "DT",
#            "product" "DP",
#            "product_class" "DPC",
#            "customer" "DC"
#        WHERE 
#            "DB"."time_id" = "DT"."time_id"
#        AND "DB"."customer_id" = "DC"."customer_id"
#        AND "DB"."product_id" = "DP"."product_id"
#        AND "DP"."product_class_id" = "DPC"."product_class_id") AS "CDC"
#    WHERE "CDC"."gender" = "C"."gender"
#    AND "CDC"."marital_status" = "C"."marital_status"
#    AND "CDC"."product_family" = "PC"."product_family"
#    AND "CDC"."product_department" = "PC"."product_department"
#    AND "CDC"."product_category" = "PC"."product_category"
#    AND "CDC"."month_of_year" = "T"."month_of_year"
#    AND "CDC"."quarter" = "T"."quarter"
#    AND "CDC"."the_year" = "T"."the_year"
#    GROUP BY 
#        "gender",
#        "marital_status",
#        "product_family",
#        "product_department",
#        "product_category",
#        "month_of_year",
#        "quarter",
#        "the_year") AS "customer_count",
#    COUNT(*) AS "fact_count"
#FROM "sales_fact_1997" "B",
#    "time_by_day" "T",
#    "product" "P",
#    "product_class" "PC",
#    "customer" "C"
#WHERE 
#    "B"."time_id" = "T"."time_id"
#AND "B"."customer_id" = "C"."customer_id"
#AND "B"."product_id" = "P"."product_id"
#AND "P"."product_class_id" = "PC"."product_class_id"
#GROUP BY 
#    "C"."gender",
#    "C"."marital_status",
#    "PC"."product_family",
#    "PC"."product_department",
#    "PC"."product_category",
#    "T"."month_of_year",
#    "T"."quarter",
#    "T"."the_year";

# End insert.sql
