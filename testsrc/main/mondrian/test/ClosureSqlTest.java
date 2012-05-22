/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2012 Pentaho and others
// All Rights Reserved.
*/
package mondrian.test;

import mondrian.rolap.BatchTestCase;
import mondrian.spi.Dialect;

public class ClosureSqlTest extends BatchTestCase {
    /**
     * This test makes sure that the closure table is used
     * when it is defined in the schema.
     */
    public void testClosureSql_1() throws Exception {
        final String mdx =
            "select {Hierarchize([Employee].[Employees].Members)} on columns from [HR]";
        final String mySql =
            "select `employee_closure`.`employee_id` as `c0`, `employee`.`full_name` as `c1` from `employee` as `employee`, `employee_closure` as `employee_closure` where `employee_closure`.`employee_id` = `employee`.`employee_id` and `employee_closure`.`supervisor_id` = 42 and `employee_closure`.`distance` = 1 group by `employee_closure`.`employee_id`, `employee`.`full_name`";
        SqlPattern[] patterns = {
            new SqlPattern(
                Dialect.DatabaseProduct.MYSQL, mySql, mySql)
        };
        assertQuerySql(
            mdx,
            patterns);
    }
}
// End ClosureSqlTest.java