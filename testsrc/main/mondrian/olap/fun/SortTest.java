/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2008 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.test.FoodMartTestCase;

/**
 * <code>SortTest</code> tests the collation order of positive and negative
 * infinity, and {@link Double#NaN}.
 *
 * @author jhyde
 * @version $Id$
 * @since Sep 21, 2006
 */
public class SortTest extends FoodMartTestCase
{
    public void testFoo() {
        // Check that each value compares according to its position in the total
        // order. For example, NaN compares greater than
        // Double.NEGATIVE_INFINITY, -34.5, -0.001, 0, 0.00000567, 1, 3.14;
        // equal to NaN; and less than Double.POSITIVE_INFINITY.
        double[] values = {
            Double.NEGATIVE_INFINITY,
            FunUtil.DoubleNull,
            -34.5,
            -0.001,
            0,
            0.00000567,
            1,
            3.14,
            Double.NaN,
            Double.POSITIVE_INFINITY,
        };
        for (int i = 0; i < values.length; i++) {
            for (int j = 0; j < values.length; j++) {
                int expected = i < j ? -1 : i == j ? 0 : 1;
                assertEquals(
                    "values[" + i +  "]=" + values[i] + ", values[" + j +
                        "]=" + values[j],
                    expected,
                    FunUtil.compareValues(values[i], values[j]));
            }
        }
    }

    public void testOrderDesc() {
        // In MSAS, NULLs collate last (or almost last, along with +inf and
        // NaN) whereas in Mondrian NULLs collate least (that is, before -inf).
        assertQueryReturns("with" +
            "   member [Measures].[Foo] as '\n" +
            "      Iif([Promotion Media].CurrentMember IS [Promotion Media].[TV], 1.0 / 0.0,\n" +
            "         Iif([Promotion Media].CurrentMember IS [Promotion Media].[Radio], -1.0 / 0.0,\n" +
            "            Iif([Promotion Media].CurrentMember IS [Promotion Media].[Bulk Mail], 0.0 / 0.0,\n" +
            "               Iif([Promotion Media].CurrentMember IS [Promotion Media].[Daily Paper], NULL,\n" +
            "       [Measures].[Unit Sales])))) '\n" +
            "select \n" +
            "    {[Measures].[Foo]} on columns, \n" +
            "    order(except([Promotion Media].[Media Type].members,{[Promotion Media].[Media Type].[No Media]}),[Measures].[Foo],DESC) on rows\n" +
            "from Sales",
            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Foo]}\n" +
                "Axis #2:\n" +
                "{[Promotion Media].[All Media].[TV]}\n" +
                "{[Promotion Media].[All Media].[Bulk Mail]}\n" +
                "{[Promotion Media].[All Media].[Daily Paper, Radio, TV]}\n" +
                "{[Promotion Media].[All Media].[Product Attachment]}\n" +
                "{[Promotion Media].[All Media].[Daily Paper, Radio]}\n" +
                "{[Promotion Media].[All Media].[Cash Register Handout]}\n" +
                "{[Promotion Media].[All Media].[Sunday Paper, Radio]}\n" +
                "{[Promotion Media].[All Media].[Street Handout]}\n" +
                "{[Promotion Media].[All Media].[Sunday Paper]}\n" +
                "{[Promotion Media].[All Media].[In-Store Coupon]}\n" +
                "{[Promotion Media].[All Media].[Sunday Paper, Radio, TV]}\n" +
                "{[Promotion Media].[All Media].[Radio]}\n" +
                "{[Promotion Media].[All Media].[Daily Paper]}\n" +
                "Row #0: Infinity\n" +
                "Row #1: NaN\n" +
                "Row #2: 9,513\n" +
                "Row #3: 7,544\n" +
                "Row #4: 6,891\n" +
                "Row #5: 6,697\n" +
                "Row #6: 5,945\n" +
                "Row #7: 5,753\n" +
                "Row #8: 4,339\n" +
                "Row #9: 3,798\n" +
                "Row #10: 2,726\n" +
                "Row #11: -Infinity\n" +
                "Row #12: \n"));
    }

    public void testOrderAndRank() {
        assertQueryReturns("with " +
            "   member [Measures].[Foo] as '\n" +
            "      Iif([Promotion Media].CurrentMember IS [Promotion Media].[TV], 1.0 / 0.0,\n" +
            "         Iif([Promotion Media].CurrentMember IS [Promotion Media].[Radio], -1.0 / 0.0,\n" +
            "            Iif([Promotion Media].CurrentMember IS [Promotion Media].[Bulk Mail], 0.0 / 0.0,\n" +
            "               Iif([Promotion Media].CurrentMember IS [Promotion Media].[Daily Paper], NULL,\n" +
            "                  [Measures].[Unit Sales])))) '\n" +
            "   member [Measures].[R] as '\n" +
            "      Rank([Promotion Media].CurrentMember, [Promotion Media].Members, [Measures].[Foo]) '\n" +
            "select\n" +
            "    {[Measures].[Foo], [Measures].[R]} on columns, \n" +
            "    order([Promotion Media].[Media Type].members,[Measures].[Foo]) on rows\n" +
            "from Sales",
            fold("Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Foo]}\n" +
                "{[Measures].[R]}\n" +
                "Axis #2:\n" +
                "{[Promotion Media].[All Media].[Daily Paper]}\n" +
                "{[Promotion Media].[All Media].[Radio]}\n" +
                "{[Promotion Media].[All Media].[Sunday Paper, Radio, TV]}\n" +
                "{[Promotion Media].[All Media].[In-Store Coupon]}\n" +
                "{[Promotion Media].[All Media].[Sunday Paper]}\n" +
                "{[Promotion Media].[All Media].[Street Handout]}\n" +
                "{[Promotion Media].[All Media].[Sunday Paper, Radio]}\n" +
                "{[Promotion Media].[All Media].[Cash Register Handout]}\n" +
                "{[Promotion Media].[All Media].[Daily Paper, Radio]}\n" +
                "{[Promotion Media].[All Media].[Product Attachment]}\n" +
                "{[Promotion Media].[All Media].[Daily Paper, Radio, TV]}\n" +
                "{[Promotion Media].[All Media].[No Media]}\n" +
                "{[Promotion Media].[All Media].[Bulk Mail]}\n" +
                "{[Promotion Media].[All Media].[TV]}\n" +
                "Row #0: \n" +
                "Row #0: 15\n" +
                "Row #1: -Infinity\n" +
                "Row #1: 14\n" +
                "Row #2: 2,726\n" +
                "Row #2: 13\n" +
                "Row #3: 3,798\n" +
                "Row #3: 12\n" +
                "Row #4: 4,339\n" +
                "Row #4: 11\n" +
                "Row #5: 5,753\n" +
                "Row #5: 10\n" +
                "Row #6: 5,945\n" +
                "Row #6: 9\n" +
                "Row #7: 6,697\n" +
                "Row #7: 8\n" +
                "Row #8: 6,891\n" +
                "Row #8: 7\n" +
                "Row #9: 7,544\n" +
                "Row #9: 6\n" +
                "Row #10: 9,513\n" +
                "Row #10: 5\n" +
                "Row #11: 195,448\n" +
                "Row #11: 4\n" +
                "Row #12: NaN\n" +
                "Row #12: 2\n" +
                "Row #13: Infinity\n" +
                "Row #13: 1\n"));
    }
}

// End SortTest.java
