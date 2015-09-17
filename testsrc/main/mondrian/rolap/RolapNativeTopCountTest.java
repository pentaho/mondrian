/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2015-2015 Pentaho ant others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.test.TestContext;

/**
 * @author Andrey Khayrutdinov
 */
public class RolapNativeTopCountTest extends BatchTestCase {

    public void setUp() throws Exception {
        super.setUp();
        propSaver.set(propSaver.properties.EnableNativeTopCount, true);
    }

    public void testTopCount_ImplicitCountMeasure() throws Exception {
        final String query = ""
            + "SELECT [Measures].[Fact Count] ON COLUMNS, "
            + "TOPCOUNT([Store Type].[All Store Types].Children, 3, [Measures].[Fact Count]) ON ROWS "
            + "FROM [Store]";

        final String expectedResult = ""
            + "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Fact Count]}\n"
            + "Axis #2:\n"
            + "{[Store Type].[Supermarket]}\n"
            + "{[Store Type].[Deluxe Supermarket]}\n"
            + "{[Store Type].[Mid-Size Grocery]}\n"
            + "Row #0: 8\n"
            + "Row #1: 6\n"
            + "Row #2: 4\n";

        TestContext.instance().assertQueryReturns(query, expectedResult);
    }

    public void testTopCount_CountMeasure() throws Exception {
        final String cube = ""
            + "  <Cube name=\"StoreWithCountM\" visible=\"true\" cache=\"true\" enabled=\"true\">\n"
            + "    <Table name=\"store\">\n"
            + "    </Table>\n"
            + "    <Dimension visible=\"true\" highCardinality=\"false\" name=\"Store Type\">\n"
            + "      <Hierarchy visible=\"true\" hasAll=\"true\">\n"
            + "        <Level name=\"Store Type\" visible=\"true\" column=\"store_type\" type=\"String\" uniqueMembers=\"true\" levelType=\"Regular\" hideMemberIf=\"Never\">\n"
            + "        </Level>\n"
            + "      </Hierarchy>\n"
            + "    </Dimension>\n"
            + "    <DimensionUsage source=\"Store\" name=\"Store\" visible=\"true\" highCardinality=\"false\">\n"
            + "    </DimensionUsage>\n"
            + "    <Dimension visible=\"true\" highCardinality=\"false\" name=\"Has coffee bar\">\n"
            + "      <Hierarchy visible=\"true\" hasAll=\"true\">\n"
            + "        <Level name=\"Has coffee bar\" visible=\"true\" column=\"coffee_bar\" type=\"Boolean\" uniqueMembers=\"true\" levelType=\"Regular\" hideMemberIf=\"Never\">\n"
            + "        </Level>\n"
            + "      </Hierarchy>\n"
            + "    </Dimension>\n"
            + "    <Measure name=\"Store Sqft\" column=\"store_sqft\" formatString=\"#,###\" aggregator=\"sum\">\n"
            + "    </Measure>\n"
            + "    <Measure name=\"Grocery Sqft\" column=\"grocery_sqft\" formatString=\"#,###\" aggregator=\"sum\">\n"
            + "    </Measure>\n"
            + "    <Measure name=\"CountM\" column=\"store_id\" formatString=\"Standard\" aggregator=\"count\" visible=\"true\">\n"
            + "    </Measure>\n"
            + "  </Cube>";

        final String schema = TestContext.instance()
            .getSchema(null, cube, null, null, null, null);

        TestContext ctx = TestContext.instance()
            .withSchema(schema)
            .withCube("StoreWithCountM");

        final String query = ""
            + "SELECT [Measures].[CountM] ON COLUMNS, "
            + "TOPCOUNT([Store Type].[All Store Types].Children, 3, [Measures].[CountM]) ON ROWS "
            + "FROM [StoreWithCountM]";

        final String expectedResult = ""
            + "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[CountM]}\n"
            + "Axis #2:\n"
            + "{[Store Type].[Supermarket]}\n"
            + "{[Store Type].[Deluxe Supermarket]}\n"
            + "{[Store Type].[Mid-Size Grocery]}\n"
            + "Row #0: 8\n"
            + "Row #1: 6\n"
            + "Row #2: 4\n";

        ctx.assertQueryReturns(query, expectedResult);
    }

    public void testTopCount_SumMeasure() throws Exception {
        final String query = ""
            + "SELECT [Measures].[Store Sqft] ON COLUMNS, "
            + "TOPCOUNT([Store Type].[All Store Types].Children, 3, [Measures].[Store Sqft]) ON ROWS "
            + "FROM [Store]";

        final String expectedResult = ""
            + "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Store Sqft]}\n"
            + "Axis #2:\n"
            + "{[Store Type].[Supermarket]}\n"
            + "{[Store Type].[Deluxe Supermarket]}\n"
            + "{[Store Type].[Mid-Size Grocery]}\n"
            + "Row #0: 193,480\n"
            + "Row #1: 146,045\n"
            + "Row #2: 109,343\n";

        TestContext.instance().assertQueryReturns(query, expectedResult);
    }
}

// End RolapNativeTopCountTest.java
