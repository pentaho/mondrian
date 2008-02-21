/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
*/

package mondrian.test;

import mondrian.olap.MondrianProperties;

/**
 * Test ignoring of measure when unrelated Dimension is in
 * aggregation list when IgnoreMeasureForNonJoiningDimension property is
 * set to true.
 * @author ajoglekar
 * @since Dec 12, 2007
 * @version $Id$
 */
public class IgnoreMeasureForNonJoiningDimensionInAggregationTest extends FoodMartTestCase {

    boolean originalNonEmptyFlag;
    boolean originalEliminateUnrelatedDimensions;
    private final MondrianProperties prop = MondrianProperties.instance();

    protected void setUp() throws Exception {
        originalNonEmptyFlag =
            prop.EnableNonEmptyOnAllAxis.get();
        originalEliminateUnrelatedDimensions =
            prop.IgnoreMeasureForNonJoiningDimension.get();
        prop.EnableNonEmptyOnAllAxis.set(true);
        prop.IgnoreMeasureForNonJoiningDimension.set(true);
    }

    protected void tearDown() throws Exception {
        prop.EnableNonEmptyOnAllAxis.set(originalNonEmptyFlag);
        prop.IgnoreMeasureForNonJoiningDimension
            .set(originalEliminateUnrelatedDimensions);
    }

    public void testNoTotalsForCompoundMeasureWithComponentsHavingNonJoiningDims() {
        assertQueryReturns("with member [Measures].[Total Sales] as " +
            "'[Measures].[Store Sales] + [Measures].[Warehouse Sales]'" +
            "member [Product].x as 'sum({Product.members  * Gender.members})' " +
            "select {[Measures].[Total Sales]} on 0, " +
            "{Product.x} on 1 from [Warehouse and Sales]",
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Total Sales]}\n" +
                "Axis #2:\n" +
                "{[Product].[x]}\n" +
                "Row #0: 7,913,333.82\n"));
    }

    public void testIgnoreMeasureForNonJoiningDimsWhenAggFunctionIsUsedOrNotUsed() {
        final String query = "WITH\n" +
            "MEMBER [Measures].[Total Sales] AS " +
            "'[Measures].[Store Sales] + [Measures].[Warehouse Sales]'\n" +
            "MEMBER [Warehouse].[AggSP1] AS\n" +
            "'IIF([Measures].CURRENTMEMBER IS [Measures].[Total Sales],\n" +
            "([Warehouse].[All Warehouses], [Measures].[Total Sales]),\n" +
            "([Product].[All Products], [Warehouse].[All Warehouses]))'\n" +
            "MEMBER [Warehouse].[AggPreSP] AS\n" +
            "'IIF([Measures].CURRENTMEMBER IS [Measures].[Total Sales],\n" +
            "([Warehouse].[All Warehouses], [Measures].[Total Sales]),\n" +
            "Aggregate({([Product].[All Products], [Warehouse].[All Warehouses])}))'\n" +
            "\n" +
            "SELECT\n" +
            "{[Measures].[Total Sales]} ON AXIS(0),\n" +
            "{{([Warehouse].[AggPreSP])},\n" +
            "{([Warehouse].[AggSP1])}} ON AXIS(1)\n" +
            "FROM\n" +
            "[Warehouse and Sales]";

        assertQueryReturns(query,
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Total Sales]}\n" +
                "Axis #2:\n" +
                "{[Warehouse].[AggPreSP]}\n" +
                "{[Warehouse].[AggSP1]}\n" +
                "Row #0: 196,770.89\n" +
                "Row #1: 196,770.89\n"));
        prop.IgnoreMeasureForNonJoiningDimension.set(false);
        assertQueryReturns(query,
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Total Sales]}\n" +
                "Axis #2:\n" +
                "{[Warehouse].[AggPreSP]}\n" +
                "{[Warehouse].[AggSP1]}\n" +
                "Row #0: 762,009.02\n" +
                "Row #1: 762,009.02\n"));
    }

    public void testIgnoreMeasureForNonJoiningDimForAMemberDefinedOnJoiningDim() {
        assertQueryReturns("WITH\n" +
            "MEMBER [Measures].[Total Sales] AS '[Measures].[Store Sales] + " +
            "[Measures].[Warehouse Sales]'\n" +
            "MEMBER [Product].[AggSP1] AS\n" +
            "'IIF([Measures].CURRENTMEMBER IS [Measures].[Total Sales],\n" +
            "([Warehouse].[All Warehouses], [Measures].[Total Sales]),\n" +
            "([Warehouse].[All Warehouses]))'\n" +
            "\n" +
            "SELECT\n" +
            "{[Measures].[Total Sales]} ON AXIS(0),\n" +
            "{[Product].[AggSP1]} ON AXIS(1)\n" +
            "FROM\n" +
            "[Warehouse and Sales]",
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Total Sales]}\n" +
                "Axis #2:\n" +
                "{[Product].[AggSP1]}\n" +
                "Row #0: 196,770.89\n"));
    }

    public void testNoTotalsForCompoundMeasureWithNonJoiningDimAtAllLevel() {
        assertQueryReturns("with member [Measures].[Total Sales] as " +
            "'[Measures].[Store Sales]'" +
            "member [Product].x as 'sum({Product.members  * " +
            "Gender.[All Gender]})' " +
            "select {[Measures].[Total Sales]} on 0, " +
            "{Product.x} on 1 from [Warehouse and Sales]",
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Total Sales]}\n" +
                "Axis #2:\n" +
                "{[Product].[x]}\n" +
                "Row #0: 3,956,666.91\n"));
    }

    public void testNoTotalForMeasureWithCrossJoinOfJoiningAndNonJoiningDims() {
        assertQueryReturns("with member [Product].x as " +
            "'sum({Product.members}  * {Gender.members})' " +
            "select {[Measures].[Warehouse Sales]} on 0, " +
            "{Product.x} on 1 from [Warehouse and Sales]",
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "Axis #2:\n"));
    }

    public void testShouldTotalAMeasureWithAllJoiningDimensions() {
        assertQueryReturns("with member [Product].x as " +
            "'sum({Product.members})' " +
            "select " +
            "{[Measures].[Warehouse Sales]} on 0, " +
            "{Product.x} on 1 " +
            "from [Warehouse and Sales]",
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "{[Measures].[Warehouse Sales]}\n" +
                "Axis #2:\n" +
                "{[Product].[x]}\n" +
                "Row #0: 1,377,396.213\n"));
    }

    public void testShouldNotTotalAMeasureWithANonJoiningDimension() {
        assertQueryReturns("with member [Gender].x as 'sum({Gender.members})'" +
            "select " +
            "{[Measures].[Warehouse Sales]} on 0, " +
            "{Gender.x} on 1 " +
            "from [Warehouse and Sales]",
            fold(
                "Axis #0:\n" +
                "{}\n" +
                "Axis #1:\n" +
                "Axis #2:\n"));
    }
}