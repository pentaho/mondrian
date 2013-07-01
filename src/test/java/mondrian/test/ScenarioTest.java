/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2009-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.test;

import mondrian.olap.Property;
import mondrian.olap.Result;
import mondrian.olap.Util;

import org.olap4j.*;

import java.sql.SQLException;
import java.util.Arrays;

/**
 * Test for writeback functionality.
 *
 * @author jhyde
 * @since 24 April, 2009
 */
public class ScenarioTest extends FoodMartTestCase {
    /**
     * Tests creating a scenario and setting a connection's active scenario.
     */
    public void testCreateScenario() throws SQLException {
        final OlapConnection connection =
            getTestContext().getOlap4jConnection();
        try {
            assertNull(connection.getScenario());
            final Scenario scenario = connection.createScenario();
            assertNotNull(scenario);
            connection.setScenario(scenario);
            assertSame(scenario, connection.getScenario());
            connection.setScenario(null);
            assertNull(connection.getScenario());
            final Scenario scenario2 = connection.createScenario();
            assertNotNull(scenario2);
            connection.setScenario(scenario2);
        } finally {
            connection.setScenario(null);
        }
    }

    /**
     * Tests setting the value of one cell.
     */
    public void testSetCell() throws SQLException {
        final OlapConnection connection =
            getTestContext().getOlap4jConnection();
        try {
            assertNull(connection.getScenario());
            final Scenario scenario = connection.createScenario();
            connection.setScenario(scenario);
            connection.prepareOlapStatement(
                "select {[Measures].[Unit Sales]} on 0,\n"
                + "{[Product].Children} on 1\n"
                + "from [Sales]");
        } finally {
            connection.setScenario(null);
        }
    }

    /**
     * Tests that setting a cell's value without an active scenario is illegal.
     */
    public void testSetCellWithoutScenarioFails() throws SQLException {
        final OlapConnection connection =
            getTestContext().getOlap4jConnection();
        try {
            assertNull(connection.getScenario());
            final PreparedOlapStatement pstmt = connection.prepareOlapStatement(
                "select {[Measures].[Unit Sales]} on 0,\n"
                + "{[Product].Children} on 1\n"
                + "from [Sales]");
            final CellSet result = pstmt.executeQuery();
            final Cell cell = result.getCell(Arrays.asList(0, 1));
            try {
                cell.setValue(123, AllocationPolicy.EQUAL_ALLOCATION);
                fail("expected error");
            } catch (RuntimeException e) {
                TestContext.checkThrowable(e, "No active scenario");
            }
        } finally {
            connection.setScenario(null);
        }
    }

    /**
     * Tests that setting a calculated member is illegal.
     */
    public void testSetCellCalcError() throws SQLException {
        final TestContext testContext = TestContext.instance().withScenario();
        final OlapConnection connection = testContext.getOlap4jConnection();
        PreparedOlapStatement pstmt = connection.prepareOlapStatement(
            "with member [Measures].[Unit Sales Plus One]\n"
            + "   as ' [Measures].[Unit Sales] + 1 '\n"
            + "select {[Measures].[Unit Sales Plus One]} on 0,\n"
            + "{[Product].Children} on 1\n"
            + "from [Sales]");
        CellSet cellSet = pstmt.executeQuery();
        Cell cell = cellSet.getCell(Arrays.asList(0, 1));
        try {
            cell.setValue(123, AllocationPolicy.EQUAL_ALLOCATION);
            fail("expected exception");
        } catch (RuntimeException e) {
            TestContext.checkThrowable(
                e,
                "Cannot write to cell: one of the coordinates "
                + "([Measures].[Unit Sales Plus One]) is a calculated member");
        }

        // Calc member on non-measures dimension
        cellSet = pstmt.executeOlapQuery(
            "with member [Product].[FoodDrink]\n"
            + "   as Aggregate({[Product].[Food], [Product].[Drink]})\n"
            + "select {[Measures].[Unit Sales]} on 0,\n"
            + "{[Product].Children, [Product].[FoodDrink]} on 1\n"
            + "from [Sales]");
        // OK to set ([Measures].[Unit Sales], [Product].[Drink])
        cell = cellSet.getCell(Arrays.asList(0, 1));
        cell.setValue(123, AllocationPolicy.EQUAL_ALLOCATION);
        // Not OK to set ([Measures].[Unit Sales], [Product].[FoodDrink])
        cell = cellSet.getCell(Arrays.asList(0, 3));
        try {
            cell.setValue(123, AllocationPolicy.EQUAL_ALLOCATION);
            fail("expected exception");
        } catch (RuntimeException e) {
            TestContext.checkThrowable(
                e,
                "Cannot write to cell: one of the coordinates "
                + "([Product].[Products].[FoodDrink]) is a calculated member");
        }
    }

    /**
     * Tests that allocation policies that are not supported give an error.
     */
    public void testUnsupportedAllocationPolicyFails() throws SQLException {
        final TestContext testContext = TestContext.instance().withScenario();
        final OlapConnection connection = testContext.getOlap4jConnection();
        final PreparedOlapStatement pstmt = connection.prepareOlapStatement(
            "select {[Measures].[Unit Sales]} on 0,\n"
            + "{[Product].Children} on 1\n"
            + "from [Sales]");
        final CellSet cellSet = pstmt.executeQuery();
        final Cell cell = cellSet.getCell(Arrays.asList(0, 1));
        for (AllocationPolicy policy : AllocationPolicy.values()) {
            switch (policy) {
            case EQUAL_ALLOCATION:
            case EQUAL_INCREMENT:
                continue;
            }
            try {
                cell.setValue(123, policy);
                fail("expected error");
            } catch (RuntimeException e) {
                TestContext.checkThrowable(
                    e,
                    "Allocation policy " + policy + " is not supported");
            }
        }
        try {
            cell.setValue(123, null);
            fail("expected error");
        } catch (RuntimeException e) {
            TestContext.checkThrowable(
                e, "Allocation policy must not be null");
        }
    }

    /**
     * Tests setting cells by the "equal increment" allocation policy.
     */
    public void testEqualIncrement() throws SQLException {
        assertAllocation(AllocationPolicy.EQUAL_INCREMENT);
    }

    /**
     * Tests setting cells by the "equal allocation" allocation policy.
     */
    public void testEqualAllocation() throws SQLException {
        assertAllocation(AllocationPolicy.EQUAL_ALLOCATION);
    }

    private void assertAllocation(
        final AllocationPolicy allocationPolicy) throws SQLException
    {
        final TestContext testContext = getTestContext2();
        final OlapConnection connection = testContext.getOlap4jConnection();
        final Scenario scenario = connection.getScenario();
        String id = scenario.getId();
        final PreparedOlapStatement pstmt = connection.prepareOlapStatement(
            "select {[Measures].[Unit Sales]} on 0,\n"
            + "{[Product].Children} on 1\n"
            + "from [Sales]\n"
            + "where [Scenario].["
            + id
            + "]");
        CellSet cellSet = pstmt.executeQuery();

        // Update ([Product].[Drink], [Measures].[Unit Sales])
        // from 24,597 to 23,597.
        final Cell cell = cellSet.getCell(Arrays.asList(0, 0));
        cell.setValue(23597, allocationPolicy);

        testContext.assertQueryReturns(
            "select {[Measures].[Unit Sales]} on 0,\n"
            + "{[Product].[Drink]} on 1\n"
            + "from [Sales]\n"
            + "where [Scenario].[Scenario].[" + id + "]",
            "Axis #0:\n"
            + "{[Scenario].[Scenario].[" + id + "]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[Drink]}\n"
            + "Row #0: 23,597\n");

        testContext.assertQueryReturns(
            "select {[Measures].[Unit Sales]} on 0,\n"
            + "{[Product].Children,\n"
            + " [Product].[Drink].Children,\n"
            + " [Product].[Drink].[Beverages].[Carbonated Beverages].[Soda],\n"
            + " [Product].[Drink].[Beverages].[Carbonated Beverages].[Soda].Children} on 1\n"
            + "from [Sales]"
            + "where [Scenario].[" + id + "]",
            "Axis #0:\n"
            + "{[Scenario].[Scenario].[" + id + "]}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[Drink]}\n"
            + "{[Product].[Products].[Food]}\n"
            + "{[Product].[Products].[Non-Consumable]}\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages]}\n"
            + "{[Product].[Products].[Drink].[Beverages]}\n"
            + "{[Product].[Products].[Drink].[Dairy]}\n"
            + "{[Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda]}\n"
            + "{[Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Excellent]}\n"
            + "{[Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Fabulous]}\n"
            + "{[Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Skinner]}\n"
            + "{[Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Token]}\n"
            + "{[Product].[Products].[Drink].[Beverages].[Carbonated Beverages].[Soda].[Washington]}\n"
            + (allocationPolicy == AllocationPolicy.EQUAL_INCREMENT
                ? "Row #0: 23,597\n"
                  + "Row #1: 191,940\n"
                  + "Row #2: 50,236\n"
                  + "Row #3: 6,560\n"
                  + "Row #4: 13,022\n"
                  + "Row #5: 4,015\n"
                  + "Row #6: 3,268\n"
                  + "Row #7: 708\n"
                  + "Row #8: 606\n"
                  + "Row #9: 629\n"
                  + "Row #10: 705\n"
                  + "Row #11: 620\n"
                : "Row #0: 23,597\n"
                  + "Row #1: 191,940\n"
                  + "Row #2: 50,236\n"
                  + "Row #3: 6,563\n"
                  + "Row #4: 12,990\n"
                  + "Row #5: 4,043\n"
                  + "Row #6: 3,274\n"
                  + "Row #7: 704\n"
                  + "Row #8: 612\n"
                  + "Row #9: 603\n"
                  + "Row #10: 716\n"
                  + "Row #11: 639\n"));

        // For reference here are the original values:
        // Row #0: 24,597
        // Row #1: 191,940
        // Row #2: 50,236
        // Row #3: 6,838
        // Row #4: 13,573
        // Row #5: 4,186
        // Row #6: 3,407
        // Row #7: 738
        // Row #8: 632
        // Row #9: 655
        // Row #10: 735
        // Row #11: 647

        // Create a new scenario, and show that the scenario in the slicer
        // overrides.
        final Scenario scenario2 = connection.createScenario();
        final String id2 = scenario2.getId();

        // Connection has scenario1,
        // slicer has scenario2,
        // slicer wins.
        String value;
        final OlapStatement stmt = connection.createStatement();
        cellSet =
            stmt.executeOlapQuery(
                "select {[Measures].[Unit Sales]} on 0,\n"
                + "{[Product].Children} on 1\n"
                + "from [Sales]\n"
                + "where [Scenario].["
                + id2
                + "]");
        cellSet.getCell(Arrays.asList(0, 0)).setValue(100, allocationPolicy);

        // With slicer=scenario1, value as per scenario1.
        cellSet =
            stmt.executeOlapQuery(
                "select {[Measures].[Unit Sales]} on 0,\n"
                + "{[Product].Children} on 1\n"
                + "from [Sales]\n"
                + "where [Scenario].["
                + id
                + "]");
        value = cellSet.getCell(Arrays.asList(0, 0)).getFormattedValue();
        assertEquals("23,597", value);

        // With slicer=scenario2, value as per scenario2.
        cellSet =
            stmt.executeOlapQuery(
                "select {[Measures].[Unit Sales]} on 0,\n"
                + "{[Product].Children} on 1\n"
                + "from [Sales]\n"
                + "where [Scenario].["
                + id2
                + "]");
        value = cellSet.getCell(Arrays.asList(0, 0)).getFormattedValue();
        assertEquals("100", value);

        // With no slicer, value as per connection's scenario, scenario1.
        assert connection.getScenario() == scenario;
        cellSet =
            stmt.executeOlapQuery(
                "select {[Measures].[Unit Sales]} on 0,\n"
                + "{[Product].Children} on 1\n"
                + "from [Sales]\n");
        value = cellSet.getCell(Arrays.asList(0, 0)).getFormattedValue();
        assertEquals("23,597", value);

        // Set connection's scenario to null, and we get the unmodified value.
        connection.setScenario(null);
        cellSet =
            stmt.executeOlapQuery(
                "select {[Measures].[Unit Sales]} on 0,\n"
                + "{[Product].Children} on 1\n"
                + "from [Sales]\n");
        value = cellSet.getCell(Arrays.asList(0, 0)).getFormattedValue();
        assertEquals("24,597", value);
    }

    public TestContext getTestContext2() {
        return TestContext.instance().withSubstitution(
            new Util.Function1<String, String>() {
                public String apply(String param) {
                    final String seek = "<Cube name='Sales' ";
                    int i = param.indexOf(seek);
                    if (i < 0) {
                        throw new AssertionError();
                    }
                    return param.substring(0, i + seek.length())
                        + "enableScenarios='true' "
                        + param.substring(i + seek.length());
                }
            })
            .withScenario();
    }

    /**
     * Test case for
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-815">MONDRIAN-815</a>,
     * "NPE from query if use a scenario and one of the cells is empty/null".
     */
    public void testBugMondrian815() throws SQLException {
        final TestContext testContext = getTestContext2();
        final OlapConnection connection = testContext.getOlap4jConnection();
        final Scenario scenario = connection.createScenario();
        connection.setScenario(scenario);
        final String id = scenario.getId();
        final String scenarioUniqueName = "[Scenario].[Scenario].[" + id + "]";
        final PreparedOlapStatement pstmt = connection.prepareOlapStatement(
            "select NON EMPTY [Gender].Members ON COLUMNS,\n"
            + "NON EMPTY Order([Product].[All Products].[Drink].Children,\n"
            + "[Gender].[All Gender].[F], ASC) ON ROWS\n"
            + "from [Sales]\n"
            + "where ([Customers].[All Customers].[USA].[CA].[San Francisco],\n"
            + " [Time].[1997], " + scenarioUniqueName + ")");

        // With bug MONDRIAN-815, got an NPE here, because cell (0, 1) has a
        // null value.
        final CellSet cellSet = pstmt.executeQuery();
        TestContext.assertEqualsVerbose(
            "Axis #0:\n"
            + "{[Customer].[Customers].[USA].[CA].[San Francisco], [Time].[Time].[1997], "
            + scenarioUniqueName
            + "}\n"
            + "Axis #1:\n"
            + "{[Customer].[Gender].[All Gender]}\n"
            + "{[Customer].[Gender].[F]}\n"
            + "{[Customer].[Gender].[M]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[Drink].[Beverages]}\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages]}\n"
            + "Row #0: 2\n"
            + "Row #0: \n"
            + "Row #0: 2\n"
            + "Row #1: 4\n"
            + "Row #1: 2\n"
            + "Row #1: 2\n",
            TestContext.toString(cellSet));
        cellSet.getCell(Arrays.asList(0, 1))
            .setValue(10, AllocationPolicy.EQUAL_ALLOCATION);
        cellSet.getCell(Arrays.asList(1, 0))
            .setValue(999, AllocationPolicy.EQUAL_ALLOCATION);
        final CellSet cellSet2 = pstmt.executeQuery();
        TestContext.assertEqualsVerbose(
            "Axis #0:\n"
            + "{[Customer].[Customers].[USA].[CA].[San Francisco], [Time].[Time].[1997], "
            + scenarioUniqueName
            + "}\n"
            + "Axis #1:\n"
            + "{[Customer].[Gender].[All Gender]}\n"
            + "{[Customer].[Gender].[F]}\n"
            + "{[Customer].[Gender].[M]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[Drink].[Alcoholic Beverages]}\n"
            + "{[Product].[Products].[Drink].[Beverages]}\n"
            + "Row #0: 10\n"
            + "Row #0: 5\n"
            + "Row #0: 5\n"
            + "Row #1: 1,001\n"
            + "Row #1: 999\n"
            + "Row #1: 2\n",
            TestContext.toString(cellSet2));
    }

    public void testScenarioPropertyBug1496() {
        // looking up the $scenario property for a non ScenarioCalc member
        // causes class cast exception
        // http://jira.pentaho.com/browse/MONDRIAN-1496
        Result result = TestContext.instance().executeQuery(
            "select customer.country.members on 0 from sales");

        // non calc member, should return null
        Object o = result.getAxes()[0].getPositions().get(0).get(0)
            .getPropertyValue(Property.SCENARIO);
        assertEquals(null, o);

        result = TestContext.instance().executeQuery(
            "with member Customer.Customers.cal as '1' "
            + "select {[Customer].Customers.cal} on 0 from Sales");
        // calc member, should return null
        o = result.getAxes()[0].getPositions().get(0).get(0)
            .getPropertyValue(Property.SCENARIO);
        assertEquals(null, o);
    }


    // TODO: test whether it is valid for two connections to have the same
    // active scenario

    // TODO: test that assigning a string to a numeric cell succeeds only if
    // the string contains a valid number

    // TODO: test that assigning a double to an integer cell succeeds; and some
    // other data types

    // TODO: test that EQUAL_ALLOCATION assigns to (a) cells that were
    // already empty, (b) cells that were null, (c) cells that are not visible
    // to the caller. I'm not sure that (c) works right now.
}

// End ScenarioTest.java
