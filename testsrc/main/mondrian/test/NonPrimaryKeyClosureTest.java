/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2012 Pentaho and others
// All Rights Reserved.
//
// pleckey, Mar 22, 2012
*/
package mondrian.test;

import mondrian.olap.MondrianProperties;
import mondrian.test.loader.CsvDBTestCase;

/**
 * Tests closure tables that close a parent-child hierarchy where
 * the child column is not the same as the dimension foreign key
 * or hierarchy primary key.
 */
public class NonPrimaryKeyClosureTest extends CsvDBTestCase {
    private static final String DIRECTORY =
            "testsrc/main/mondrian/test";
    private static final String NonPrimaryKeyClosureTest = "NonPrimaryKeyClosureTest.csv";

    public static final String PROP_NAME =  "mondrian.test.nonprimarykeyclosure";

    //private boolean useImplicitMembers;
    public NonPrimaryKeyClosureTest() {
        super();
    }
    public NonPrimaryKeyClosureTest(String name) {
        super(name);
    }
    protected void setUp() throws Exception {
        super.setUp();

        // turn off caching
        MondrianProperties props = MondrianProperties.instance();
        propSaver.set(
                props.DisableCaching,
                true);
    }
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testNonPrimaryKeyClosure() throws Exception {
        getCubeTestContext().assertQueryReturns(
                "SELECT"
                        + " {[Measures].[Salary]} ON COLUMNS,"
                        + " {[Store Managers.Manager].[Employees].Members} ON ROWS"
                        + " FROM [ClosureTest]",
                "Axis #0:\n"
                        + "{}\n"
                        + "Axis #1:\n"
                        + "{[Measures].[Salary]}\n"
                        + "Axis #2:\n"
                        + "{[Store Managers.Manager].[Jonathan Murraiin]}\n"
                        + "{[Store Managers.Manager].[Walter Cavestany]}\n"
                        + "{[Store Managers.Manager].[Kevin Armstrong]}\n"
                        + "{[Store Managers.Manager].[Cody Goldey]}\n"
                        + "Row #0: 221,200\n"
                        + "Row #1: 191,800\n"
                        + "Row #2: 23,220\n"
                        + "Row #3: 221,200\n"        );
    }

    protected String getDirectoryName() {
        return DIRECTORY;
    }
    protected String getFileName() {
        return NonPrimaryKeyClosureTest;
    }

    protected String getCubeDescription() {
        return
                "<Cube name=\"ClosureTest\">\n"
                        + "	<Table name=\"employee\"/>\n"
                        + "	\n"
                        + " <Dimension name=\"Store Managers\" foreignKey=\"store_id\">\n"
                        + "        <Hierarchy hasAll=\"true\" allMemberName=\"All Managers\"\n"
                        + "                   primaryKey=\"store_id\" primaryKeyTable=\"non_pkey_closure_test\"\n"
                        + "                   name=\"Manager\">\n"
                        + "            <Table name=\"non_pkey_closure_test\"/>\n"
                        + "            <Level name=\"Employees\" type=\"Numeric\" uniqueMembers=\"true\"\n"
                        + "                   column=\"employee_id\" parentColumn=\"supervisor_id\"\n"
                        + "                   nameColumn=\"full_name\" nullParentValue=\"0\">\n"
                        + "                <Closure parentColumn=\"supervisor_id\" childColumn=\"employee_id\">\n"
                        + "                    <Table name=\"employee_closure\"/>\n"
                        + "                </Closure>\n"
                        + "            </Level>\n"
                        + "        </Hierarchy>\n"
                        + "    </Dimension>\n"
                        + "    \n"
                        + "    <Measure name=\"Salary\" column=\"salary\" aggregator=\"sum\" formatString=\"Standard\"/>\n"
                        + "</Cube>\n";
    }
}
