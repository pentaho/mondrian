/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2013-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.test;

import mondrian.olap.Util;
import mondrian.util.Hook;

import net.hydromatic.optiq.MutableSchema;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.SchemaFactory;
import net.hydromatic.optiq.Table;
import net.hydromatic.optiq.impl.DelegatingSchema;
import net.hydromatic.optiq.impl.TableInSchemaImpl;
import net.hydromatic.optiq.impl.java.ReflectiveSchema;

import java.util.*;

/**
 * Unit test for data sources defined in the schema.
 */
public class DataSourceTest extends FoodMartTestCase {
    /** Tests a schema that contains data sources. */
    public void testDataSourcesInSchema() {
        final List<String> list = new ArrayList<String>();
        Hook.Closeable hook = Hook.DATA_SOURCE.add(
            new Util.Function1<String, Void>() {
                public Void apply(String param) {
                    System.out.println("model: " + param);
                    list.add(param);
                    return null;
                }
            });
        try {
            final TestContext testContext = getTestContext().withSubstitution(
                new Util.Function1<String, String>() {
                    public String apply(String param) {
                        return param.replace(
                            "<PhysicalSchema>",
                            "\n"
                            + "  <DataSource name='foodmart' type='jdbc' jdbcUser='foodmart' jdbcPassword='foodmart' jdbcUrl='jdbc:mysql://localhost/foodmart'/>\n"
                            + "  <DataSource name='my' type='custom' factory='"
                            + MySchemaFactory.class.getName()
                            + "'>\n"
                            + "    <Operands>\n"
                            + "      <Operand name='tableName'>ELVIS</Operand>\n"
                            + "    </Operands>\n"
                            + "    <DataSourceTables>\n"
                            + "      <DataSourceTable name='one_two' type='view'>\n"
                            + "        <Operands>\n"
                            + "          <Operand name='sql'>SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t (x, y)</Operand>\n"
                            + "        </Operands>\n"
                            + "      </DataSourceTable>\n"
                            + "      <DataSourceTable name='first_5_employee' type='view'>\n"
                            + "        <Operands>\n"
                            + "          <Operand name='sql'>SELECT * FROM \"foodmart\".\"employee\" WHERE \"employee_id\" &lt; 5</Operand>\n"
                            + "        </Operands>\n"
                            + "      </DataSourceTable>\n"
                            + "    </DataSourceTables>\n"
                            + "  </DataSource>\n"
                            + "<PhysicalSchema>\n")
                            .replace(
                                "<Table name='employee'>",
                                "<Table schema='my' name='first_5_employee' alias='employee'>");
                    }
                });
            testContext.assertSimpleQuery();
            String s = list.get(0);
            assertEquals(
                "{\n"
                + "  version: \"1.0\",\n"
                + "  defaultSchema: \"foodmart\",\n"
                + "  schemas: [\n"
                + "    {\n"
                + "      name: \"foodmart\",\n"
                + "      type: \"jdbc\",\n"
                + "      jdbcUser: \"foodmart\",\n"
                + "      jdbcPassword: \"foodmart\",\n"
                + "      jdbcUrl: \"jdbc:mysql://localhost/foodmart\"\n"
                + "    },\n"
                + "    {\n"
                + "      name: \"my\",\n"
                + "      type: \"custom\",\n"
                + "      factory: \"mondrian.test.DataSourceTest$MySchemaFactory\",\n"
                + "      operand: {\n"
                + "        tableName: \"ELVIS\"\n"
                + "      },\n"
                + "      tables: [\n"
                + "        {\n"
                + "          name: \"one_two\",\n"
                + "          type: \"view\",\n"
                + "          sql: \"SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t (x, y)\"\n"
                + "        },\n"
                + "        {\n"
                + "          name: \"first_5_employee\",\n"
                + "          type: \"view\",\n"
                + "          sql: \"SELECT * FROM \\\"foodmart\\\".\\\"employee\\\" WHERE \\\"employee_id\\\" < 5\"\n"
                + "        }\n"
                + "      ]\n"
                + "    }\n"
                + "  ]\n"
                + "}",
                s);

            // Should return just 5 members, if we have successfully
            // substituted "employee" table with "first_5_employee".
            testContext.assertQueryReturns(
                "select [Employee].[Employees].Members on 0\n"
                + "from [HR]",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Employee].[Employees].[All Employees]}\n"
                + "{[Employee].[Employees].[Sheri Nowmer]}\n"
                + "{[Employee].[Employees].[Sheri Nowmer].[Derrick Whelply]}\n"
                + "{[Employee].[Employees].[Sheri Nowmer].[Michael Spence]}\n"
                + "Row #0: $14,392,559.99\n"
                + "Row #0: $473,040.00\n"
                + "Row #0: $157,680.00\n"
                + "Row #0: \n");
        } finally {
            hook.close();
        }
    }

    // TODO: test that get error if schema does not contain any DataSource
    // elements and the connect string does not have username, password,
    // jdbcUrl etc.

    // TODO: test that get warning if schema contains DataSource elements
    // and connect string has jdbcUrl etc. jdbcUrl will be ignored.

    public static class MySchemaFactory implements SchemaFactory {
        public Schema create(
            MutableSchema parentSchema,
            String name,
            Map<String, Object> operand)
        {
            final ReflectiveSchema schema =
                ReflectiveSchema.create(parentSchema, name, new HrSchema());

            // Mine the EMPS table and add it under another name e.g. ELVIS
            final Table table = schema.getTable("emps", Object.class);

            String tableName = (String) operand.get("tableName");
            schema.addTable(
                new TableInSchemaImpl(
                    schema, tableName, Schema.TableType.TABLE, table));

            final Boolean mutable = (Boolean) operand.get("mutable");
            if (mutable == null || mutable) {
                return schema;
            } else {
                // Wrap the schema in DelegatingSchema so that it does not
                // implement MutableSchema.
                return new DelegatingSchema(schema);
            }
        }
    }

    public static class HrSchema {
        @Override
        public String toString() {
            return "HrSchema";
        }

        public final Employee[] emps = {
            new Employee(100, 10, "Bill", 10000, 1000),
            new Employee(200, 20, "Eric", 8000, 500),
            new Employee(150, 10, "Sebastian", 7000, null),
            new Employee(110, 10, "Theodore", 11500, 250),
        };

        public final Department[] depts = {
            new Department(10, "Sales", Arrays.asList(emps[0], emps[2])),
            new Department(30, "Marketing", Collections.<Employee>emptyList()),
            new Department(40, "HR", Collections.singletonList(emps[1])),
        };
    }

    public static class Employee {
        public final int empid;
        public final int deptno;
        public final String name;
        public final float salary;
        public final Integer commission;

        public Employee(
            int empid, int deptno, String name, float salary,
            Integer commission)
        {
            this.empid = empid;
            this.deptno = deptno;
            this.name = name;
            this.salary = salary;
            this.commission = commission;
        }

        public String toString() {
            return "Employee [empid: " + empid + ", deptno: " + deptno
                   + ", name: " + name + "]";
        }
    }

    public static class Department {
        public final int deptno;
        public final String name;
        public final List<Employee> employees;

        public Department(int deptno, String name, List<Employee> employees) {
            this.deptno = deptno;
            this.name = name;
            this.employees = employees;
        }


        public String toString() {
            return "Department [deptno: " + deptno + ", name: " + name
                   + ", employees: " + employees + "]";
        }
    }
}

// End DataSourceTest.java
