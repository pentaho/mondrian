/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2017 Hitachi Vantara..  All rights reserved.
*/

package mondrian.olap;

import mondrian.rolap.RolapUtil;
import mondrian.test.FoodMartTestCase;

import java.io.IOException;

/**
 * <code>NullMemberRepresentationTest</code> tests the null member
 * custom representation feature supported via
 * {@link mondrian.olap.MondrianProperties#NullMemberRepresentation} property.
 * @author ajogleka
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

}

// End NullMemberRepresentationTest.java
