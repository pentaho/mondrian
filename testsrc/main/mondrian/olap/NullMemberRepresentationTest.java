/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2007-2008 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap;

import mondrian.test.FoodMartTestCase;
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
                "with member [Measures].[Foo] as ' ClosingPeriod().uniquename '\n" +
                        "select {[Measures].[Foo]} on columns,\n" +
                        "  {[Time].[1997],\n" +
                        "   [Time].[1997].[Q2],\n" +
                        "   [Time].[1997].[Q2].[4]} on rows\n" +
                        "from Sales",
                fold(
                        "Axis #0:\n" +
                                "{}\n" +
                                "Axis #1:\n" +
                                "{[Measures].[Foo]}\n" +
                                "Axis #2:\n" +
                                "{[Time].[1997]}\n" +
                                "{[Time].[1997].[Q2]}\n" +
                                "{[Time].[1997].[Q2].[4]}\n" +
                                "Row #0: [Time].[1997].[Q4]\n" +
                                "Row #1: [Time].[1997].[Q2].[6]\n" +
                                "Row #2: [Time].[" +
                                getNullMemberRepresentation() +
                                "]\n" +
                                ""));
    }

    public void testItemMemberWithCustomNullMemberRepresentation()
            throws IOException {
        assertExprReturns("[Time].[1997].Children.Item(6).UniqueName",
                "[Time].["+ getNullMemberRepresentation() + "]");
        assertExprReturns("[Time].[1997].Children.Item(-1).UniqueName",
                "[Time].["+ getNullMemberRepresentation() + "]");
    }

    public void testNullMemberWithCustomRepresentation() throws IOException {
        assertExprReturns("[Gender].[All Gender].Parent.UniqueName",
                "[Gender].["+ getNullMemberRepresentation() + "]");

        assertExprReturns("[Gender].[All Gender].Parent.Name",
                getNullMemberRepresentation());

    }

    private String getNullMemberRepresentation() {
        return RolapUtil.mdxNullLiteral;
    }

}

// End NullMemberRepresentationTest.java
