/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2007-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap;

import mondrian.test.FoodMartTestCase;
import mondrian.test.TestContext;
import mondrian.rolap.RolapUtil;

import java.io.IOException;

/**
 * <code>NullMemberRepresentationTest</code> tests the null member
 * custom representation feature supported via
 * {@link mondrian.olap.MondrianProperties#NullMemberRepresentation} property.
 * @author ajogleka
 * @version $Id$
 */
public class NullMemberRepresentationTest extends FoodMartTestCase {

    public void testClosingPeriodMemberLeafWithCustomNullRepresentation() {
        assertQueryReturns(
            "with member [Measures].[Foo] as ' ClosingPeriod().uniquename '\n"
            + "select {[Measures].[Foo]} on columns,\n"
            + "  {[Time].[1997],\n"
            + "   [Time].[1997].[Q2],\n"
            + "   [Time].[1997].[Q2].[4]} on rows\n"
            + "from Sales",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Foo]}\n"
            + "Axis #2:\n"
            + "{[Time].[1997]}\n"
            + "{[Time].[1997].[Q2]}\n"
            + "{[Time].[1997].[Q2].[4]}\n"
            + "Row #0: [Time].[1997].[Q4]\n"
            + "Row #1: [Time].[1997].[Q2].[6]\n"
            + "Row #2: [Time].["
            + getNullMemberRepresentation()
            + "]\n"
            + "");
    }

    public void testItemMemberWithCustomNullMemberRepresentation()
        throws IOException
    {
        assertExprReturns(
            "[Time].[1997].Children.Item(6).UniqueName",
            "[Time].[" + getNullMemberRepresentation() + "]");
        assertExprReturns(
            "[Time].[1997].Children.Item(-1).UniqueName",
            "[Time].[" + getNullMemberRepresentation() + "]");
    }

    public void testNullMemberWithCustomRepresentation() throws IOException {
        assertExprReturns(
            "[Gender].[All Gender].Parent.UniqueName",
            "[Gender].[" + getNullMemberRepresentation() + "]");

        assertExprReturns(
            "[Gender].[All Gender].Parent.Name", getNullMemberRepresentation());
    }

    private String getNullMemberRepresentation() {
        return RolapUtil.mdxNullLiteral();
    }

    public void testCjMembersWithHideIfBlankAndMemberNameSpaceLeaf() {
        MondrianProperties properties = MondrianProperties.instance();
        String connectionString =
            (String) properties.get("mondrian.test.connectString");
        String jdbcUrl = (String) properties.get("mondrian.foodmart.jdbcURL");

        if (jdbcUrl == null
            || jdbcUrl.trim().equals("")
            || connectionString == null
            || connectionString.trim().equals(""))
        {
            return;
        }

        if (!isDbSupported(connectionString) || !isDbSupported(jdbcUrl)) {
            return;
        }
        TestContext testContext = TestContext.createSubstitutingCube(
            "Sales",
            "<Dimension name=\"Gender4\" foreignKey=\"customer_id\">\n"
                + "    <Hierarchy hasAll=\"true\" allMemberName=\"All Gender\" primaryKey=\"customer_id\">\n"
                + "      <Table name=\"customer\"/>\n"
                + "      <Level name=\"Gender\" column=\"gender\" uniqueMembers=\"true\" hideMemberIf=\"IfBlankName\">\n"
                + "         <NameExpression> "
                + " <SQL dialect='generic'> "
                +           "case \"gender\" "
                +           "when 'F' then ' ' "
                +           "when 'M' then 'M' "
                + " end "
                + "</SQL> "
                + "</NameExpression>  "
                + "      </Level>"
                + "    </Hierarchy>\n"
                + "  </Dimension>");
        testContext.assertQueryReturns(
                " select {[Gender4].[Gender].members} "
                    + "on COLUMNS "
                    + "from sales",
            "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Gender4].[All Gender].[M]}\n"
                + "Row #0: 135,215\n");
    }

    public boolean isDbSupported(String property)
    {
        return property.indexOf("oracle") != -1
            || property.indexOf("postgres") != -1
            || property.indexOf("mysql") != -1
            || property.indexOf("mssql") != -1
            || property.indexOf("derby") != -1
            || property.indexOf("teradata") != -1
            || property.indexOf("db2") != -1
            || property.indexOf("luciddb") != -1;
    }
}

// End NullMemberRepresentationTest.java
