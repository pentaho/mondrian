/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2005-2009 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap.agg;

import mondrian.test.FoodMartTestCase;
import mondrian.test.TestContext;
import mondrian.olap.MondrianProperties;

public class RolapAggTest extends FoodMartTestCase {

    public RolapAggTest() {
        super();
    }

    public RolapAggTest(String name) {
        super(name);
    }

    /**
     * Test for bug MONDRIAN-812. Using a key expression for a level
     * element would make aggregate tables fail to be used.
     */
    public void testLevelKeyAsSqlExpWithAgg() {
        MondrianProperties props = MondrianProperties.instance();
        props.AggregateRules.setString("DefaultRules.xml");
        props.UseAggregates.setString("true");
        props.ReadAggregates.setString("true");
        final TestContext testContext = TestContext.createSubstitutingCube(
            "Sales",
            "<Dimension name=\"Promotions\" foreignKey=\"promotion_id\">\n"
            + "  <Hierarchy hasAll=\"true\" allMemberName=\"All Promotions\" primaryKey=\"promotion_id\" defaultMember=\"[All Promotions]\">\n"
            + "    <Table name=\"promotion\"/>\n"
            + "    <Level name=\"Promotion Name\" uniqueMembers=\"true\">\n"
            + "      <KeyExpression><SQL>TRIM(\"promotion_name\")</SQL></KeyExpression>\n"
            + "    </Level>\n"
            + "  </Hierarchy>\n"
            + "</Dimension>");
        testContext.assertQueryReturns(
            "select {[Promotions].[All Promotions]} ON COLUMNS, "
            + "{[Store].[Store Name].Members} ON ROWS "
            + "from [Sales] "
            + "where {[Measures].[Unit Sales]}",
            "Axis #0:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #1:\n"
            + "{[Promotions].[All Promotions]}\n"
            + "Axis #2:\n"
            + "{[Store].[Canada].[BC].[Vancouver].[Store 19]}\n"
            + "{[Store].[Canada].[BC].[Victoria].[Store 20]}\n"
            + "{[Store].[Mexico].[DF].[Mexico City].[Store 9]}\n"
            + "{[Store].[Mexico].[DF].[San Andres].[Store 21]}\n"
            + "{[Store].[Mexico].[Guerrero].[Acapulco].[Store 1]}\n"
            + "{[Store].[Mexico].[Jalisco].[Guadalajara].[Store 5]}\n"
            + "{[Store].[Mexico].[Veracruz].[Orizaba].[Store 10]}\n"
            + "{[Store].[Mexico].[Yucatan].[Merida].[Store 8]}\n"
            + "{[Store].[Mexico].[Zacatecas].[Camacho].[Store 4]}\n"
            + "{[Store].[Mexico].[Zacatecas].[Hidalgo].[Store 12]}\n"
            + "{[Store].[Mexico].[Zacatecas].[Hidalgo].[Store 18]}\n"
            + "{[Store].[USA].[CA].[Alameda].[HQ]}\n"
            + "{[Store].[USA].[CA].[Beverly Hills].[Store 6]}\n"
            + "{[Store].[USA].[CA].[Los Angeles].[Store 7]}\n"
            + "{[Store].[USA].[CA].[San Diego].[Store 24]}\n"
            + "{[Store].[USA].[CA].[San Francisco].[Store 14]}\n"
            + "{[Store].[USA].[OR].[Portland].[Store 11]}\n"
            + "{[Store].[USA].[OR].[Salem].[Store 13]}\n"
            + "{[Store].[USA].[WA].[Bellingham].[Store 2]}\n"
            + "{[Store].[USA].[WA].[Bremerton].[Store 3]}\n"
            + "{[Store].[USA].[WA].[Seattle].[Store 15]}\n"
            + "{[Store].[USA].[WA].[Spokane].[Store 16]}\n"
            + "{[Store].[USA].[WA].[Tacoma].[Store 17]}\n"
            + "{[Store].[USA].[WA].[Walla Walla].[Store 22]}\n"
            + "{[Store].[USA].[WA].[Yakima].[Store 23]}\n"
            + "Row #0: \n"
            + "Row #1: \n"
            + "Row #2: \n"
            + "Row #3: \n"
            + "Row #4: \n"
            + "Row #5: \n"
            + "Row #6: \n"
            + "Row #7: \n"
            + "Row #8: \n"
            + "Row #9: \n"
            + "Row #10: \n"
            + "Row #11: \n"
            + "Row #12: 21,333\n"
            + "Row #13: 25,663\n"
            + "Row #14: 25,635\n"
            + "Row #15: 2,117\n"
            + "Row #16: 26,079\n"
            + "Row #17: 41,580\n"
            + "Row #18: 2,237\n"
            + "Row #19: 24,576\n"
            + "Row #20: 25,011\n"
            + "Row #21: 23,591\n"
            + "Row #22: 35,257\n"
            + "Row #23: 2,203\n"
            + "Row #24: 11,491\n");
    }
}

// End RolapAggTest.java