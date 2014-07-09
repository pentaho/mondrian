/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2012-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.test;

import mondrian.olap.Util;

import org.olap4j.*;

import java.sql.*;

/**
 * Unit test for hanger dimensions.
 */
public class HangerDimensionTest extends FoodMartTestCase {
    /** Unit test for a simple hanger dimension with values true and false. */
    public void testHangerDimension() {
        getTestContext()
            .insertDimension(
                "Sales",
                "<Dimension name='Boolean' hanger='true' key='Boolean'>\n"
                + "  <Attributes>\n"
                + "    <Attribute name='Boolean'/>\n"
                + "  </Attributes>\n"
                + "</Dimension>")
            .insertCalculatedMembers(
                "Sales",
                "<CalculatedMember name='False' hierarchy='[Boolean].[Boolean]' formula='[Marital Status]'/>\n"
                + "<CalculatedMember name='True' hierarchy='[Boolean].[Boolean]' formula='[Marital Status]'/>\n")
            .assertQueryReturns(
                "with member [Measures].[Store Sales2] as\n"
                + "   Iif([Boolean].[Boolean].CurrentMember is [Boolean].[Boolean].[True],\n"
                + "       ([Boolean].[All Boolean], [Measures].[Store Sales]),"
                + "       ([Boolean].[All Boolean], [Measures].[Store Sales]) - ([Boolean].[All Boolean], [Measures].[Store Cost]))\n"
                + "select [Measures].[Store Sales2] on columns,\n"
                + " [Boolean].[Boolean].AllMembers * [Gender].Children on rows\n"
                + "from [Sales]",
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Measures].[Store Sales2]}\n"
                + "Axis #2:\n"
                + "{[Boolean].[Boolean].[All Boolean], [Customer].[Gender].[F]}\n"
                + "{[Boolean].[Boolean].[All Boolean], [Customer].[Gender].[M]}\n"
                + "{[Boolean].[Boolean].[False], [Customer].[Gender].[F]}\n"
                + "{[Boolean].[Boolean].[False], [Customer].[Gender].[M]}\n"
                + "{[Boolean].[Boolean].[True], [Customer].[Gender].[F]}\n"
                + "{[Boolean].[Boolean].[True], [Customer].[Gender].[M]}\n"
                + "Row #0: 168,448.73\n"
                + "Row #1: 171,162.17\n"
                + "Row #2: 168,448.73\n"
                + "Row #3: 171,162.17\n"
                + "Row #4: 280,226.21\n"
                + "Row #5: 285,011.92\n");
    }

    /** Unit test that if a hierarchy has no real members, only calculated
     * members, then the default member is the first calculated member. */
    public void testHangerDimensionImplicitCalculatedDefaultMember() {
        getTestContext()
            .insertDimension(
                "Sales",
                "<Dimension name='Boolean' hanger='true' key='Boolean'>\n"
                + "  <Attributes>\n"
                + "    <Attribute name='Boolean' hierarchyHasAll='false'/>\n"
                + "  </Attributes>\n"
                + "</Dimension>")
            .insertCalculatedMembers(
                "Sales",
                "<CalculatedMember name='False' hierarchy='[Boolean].[Boolean]' formula='[Marital Status]'/>\n"
                + "<CalculatedMember name='True' hierarchy='[Boolean].[Boolean]' formula='[Marital Status]'/>\n")
            .assertAxisReturns(
                "[Boolean].[Boolean]",
                "[Boolean].[Boolean].[False]");
    }

    /** Tests that it is an error if an attribute has no members.
     * (No all member, no real members, no calculated members.) */
    public void testHangerDimensionEmptyIsError() {
        getTestContext()
            .insertDimension(
                "Sales",
                "<Dimension name='Boolean' hanger='true' key='Boolean'>\n"
                + "  <Attributes>\n"
                + "    <Attribute name='Boolean' hierarchyHasAll='false'/>\n"
                + "  </Attributes>\n"
                + "</Dimension>")
            .assertErrorList().containsError(
                "mondrian.olap.InvalidHierarchyException: Mondrian Error:Hierarchy '\\[Boolean\\].\\[Boolean\\]' is invalid \\(has no members\\) \\(in Attribute 'Boolean'\\) \\(at ${pos}\\).*",
                "<Attribute name='Boolean' hierarchyHasAll='false'/>");
    }

    /** Tests that it is an error if an attribute in a hanger dimension has a
     * keyColumn specified. (All other mappings to columns, e.g. nameColumn
     * or included Key element, are illegal too.) */
    public void testHangerDimensionKeyColumnNotAllowed() {
        getTestContext()
            .insertDimension(
                "Sales",
                "<Dimension name='Boolean' hanger='true' key='Boolean'>\n"
                + "  <Attributes>\n"
                + "    <Attribute name='Boolean' keyColumn='xxx'/>\n"
                + "  </Attributes>\n"
                + "</Dimension>")
            .assertErrorList().containsError(
                "Attribute 'Boolean' in hanger dimension must not map to column \\(in Attribute 'Boolean'\\) \\(at ${pos}\\)",
                "<Attribute name='Boolean' keyColumn='xxx'/>");
    }

    /** Tests drill-through of a query involving a hanger dimension. */
    public void testHangerDimensionDrillThrough() throws SQLException {
        OlapConnection connection = null;
        OlapStatement statement = null;
        CellSet cellSet = null;
        ResultSet resultSet = null;
        try {
            connection = getTestContext()
                .insertDimension(
                    "Sales",
                    "<Dimension name='Boolean' hanger='true' key='Boolean'>\n"
                    + "  <Attributes>\n"
                    + "    <Attribute name='Boolean'/>\n"
                    + "  </Attributes>\n"
                    + "</Dimension>")
                .insertCalculatedMembers(
                    "Sales",
                    "<CalculatedMember name='False' hierarchy='[Boolean].[Boolean]' formula='[Marital Status]'/>\n"
                    + "<CalculatedMember name='True' hierarchy='[Boolean].[Boolean]' formula='[Marital Status]'/>\n")
                .getOlap4jConnection();
            statement = connection.createStatement();
            cellSet =
                statement.executeOlapQuery(
                    "select [Gender].Members on 0,\n"
                    + "[Boolean].Members on 1\n"
                    + "from [Sales]");
            resultSet = cellSet.getCell(0).drillThrough();
            int n = 0;
            while (resultSet.next()) {
                ++n;
            }
            assertEquals(12, n);
        } finally {
            Util.close(resultSet, null, null);
            Util.close(cellSet, statement, connection);
        }
    }
}

// End HangerDimensionTest.java
