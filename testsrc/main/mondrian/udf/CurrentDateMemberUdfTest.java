/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2010-2011 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.udf;

import mondrian.test.FoodMartTestCase;
import mondrian.test.TestContext;

/**
 * Tests the CurrentDateMemberUdf class.
 *
 * @author Luc Boudreau
 * @version $Id$
 */
public class CurrentDateMemberUdfTest extends FoodMartTestCase {
    public CurrentDateMemberUdfTest() {
        super();
    }
    public CurrentDateMemberUdfTest(String name) {
        super(name);
    }

    public void testCurrentDateMemberUdf() {
        TestContext context = TestContext.instance().create(
            null,
            null,
            null,
            null,
            "<UserDefinedFunction name=\"MockCurrentDateMember\" "
            + "className=\"mondrian.udf.MockCurrentDateMember\" /> ",
            null);
        context.assertQueryReturns(
            "SELECT NON EMPTY {[Measures].[Org Salary]} ON COLUMNS, "
            + "NON EMPTY {MockCurrentDateMember([Time].[Time], \"[yyyy]\")} ON ROWS "
            + "FROM [HR] ",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Org Salary]}\n"
            + "Axis #2:\n"
            + "{[Time].[Time].[1997]}\n"
            + "Row #0: $39,431.67\n");
    }
}

// End CurrentDateMemberUdfTest.java
