/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2007-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.olap;

import mondrian.pref.PrefDef;
import mondrian.rolap.RolapUtil;
import mondrian.test.FoodMartTestCase;

import java.io.IOException;

/**
 * <code>NullMemberRepresentationTest</code> tests the null member
 * custom representation feature supported via the
 * {@link PrefDef#NullMemberRepresentation} property.
 *
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
            + "{[Time].[Time].[1997]}\n"
            + "{[Time].[Time].[1997].[Q2]}\n"
            + "{[Time].[Time].[1997].[Q2].[4]}\n"
            + "Row #0: [Time].[Time].[1997].[Q4]\n"
            + "Row #1: [Time].[Time].[1997].[Q2].[6]\n"
            + "Row #2: [Time].[Time].["
            + getNullMemberRepresentation()
            + "]\n"
            + "");
    }

    public void testItemMemberWithCustomNullMemberRepresentation()
        throws IOException
    {
        assertExprReturns(
            "[Time].[1997].Children.Item(6).UniqueName",
            "[Time].[Time].[" + getNullMemberRepresentation() + "]");
        assertExprReturns(
            "[Time].[1997].Children.Item(-1).UniqueName",
            "[Time].[Time].[" + getNullMemberRepresentation() + "]");
    }

    public void testNullMemberWithCustomRepresentation() throws IOException {
        assertExprReturns(
            "[Gender].[All Gender].Parent.UniqueName",
            "[Customer].[Gender].[" + getNullMemberRepresentation() + "]");

        assertExprReturns(
            "[Gender].[All Gender].Parent.Name", getNullMemberRepresentation());
    }

    private String getNullMemberRepresentation() {
        return RolapUtil.mdxNullLiteral();
    }

}

// End NullMemberRepresentationTest.java
