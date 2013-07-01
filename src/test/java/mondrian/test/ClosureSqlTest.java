/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2012-2012 Pentaho and others
// All Rights Reserved.
*/
package mondrian.test;

import mondrian.rolap.BatchTestCase;

public class ClosureSqlTest extends BatchTestCase {
    /**
     * Test that makes sure that the closure table is used
     * when it is defined in the schema.
     */
    public void testClosureSql_1() throws Exception {
        final String mdx =
            "select {Hierarchize([Employee].[Employees].Members)} on columns from [HR]";
        final String mySql =
            "select\n"
            + "    `employee_closure`.`employee_id` as `c0`,\n"
            + "    `employee`.`full_name` as `c1`\n"
            + "from\n"
            + "    `employee` as `employee`,\n"
            + "    `employee_closure` as `employee_closure`\n"
            + "where\n"
            + "    `employee_closure`.`employee_id` = `employee`.`employee_id`\n"
            + "and\n"
            + "    `employee_closure`.`supervisor_id` = 42\n"
            + "and\n"
            + "    `employee_closure`.`distance` = 1\n"
            + "group by\n"
            + "    `employee_closure`.`employee_id`,\n"
            + "    `employee`.`full_name`";
        assertQuerySql(getTestContext(), mdx, mySql);
    }

}

// End ClosureSqlTest.java
