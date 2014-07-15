/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2012-2014 Pentaho and others
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
            + "    `employee_closure`.`supervisor_id` as `c0`,\n"
            + "    `time_by_day`.`the_year` as `c1`,\n"
            + "    sum(`salary`.`salary_paid`) as `m0`\n"
            + "from\n"
            + "    `salary` as `salary`,\n"
            + "    `employee_closure` as `employee_closure`,\n"
            + "    `time_by_day` as `time_by_day`\n"
            + "where\n"
            + "    `time_by_day`.`the_year` = 1997\n"
            + "and\n"
            + "    `salary`.`employee_id` = `employee_closure`.`employee_id`\n"
            + "and\n"
            + "    `salary`.`pay_date` = `time_by_day`.`the_date`\n"
            + "group by\n"
            + "    `employee_closure`.`supervisor_id`,\n"
            + "    `time_by_day`.`the_year`";
        assertQuerySql(getTestContext(), mdx, mySql);
    }

}

// End ClosureSqlTest.java
