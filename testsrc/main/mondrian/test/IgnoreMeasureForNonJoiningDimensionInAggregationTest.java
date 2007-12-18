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
 * set to true
 * @author ajoglekar
 * @since Dec 12, 2007
 * @version $Id$
 */
public class IgnoreMeasureForNonJoiningDimensionInAggregationTest extends FoodMartTestCase {

    boolean originalNonEmptyFlag;
    boolean originalEliminateUnrelatedDimensions;

    protected void setUp() throws Exception {
        originalNonEmptyFlag =
            MondrianProperties.instance().EnableNonEmptyOnAllAxis.get();
        originalEliminateUnrelatedDimensions =
            MondrianProperties.instance().IgnoreMeasureForNonJoiningDimension.get();
        MondrianProperties.instance().EnableNonEmptyOnAllAxis.set(true);
        MondrianProperties.instance().IgnoreMeasureForNonJoiningDimension.set(true);
    }

    protected void tearDown() throws Exception {
        MondrianProperties.instance()
            .EnableNonEmptyOnAllAxis.set(originalNonEmptyFlag);
        MondrianProperties.instance()
            .IgnoreMeasureForNonJoiningDimension
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

    public void testNoTotalsForCompoundMeasureWithNonJoiningDimAtAllLevel() {
        assertQueryReturns("with member [Measures].[Total Sales] as " +
            "'[Measures].[Store Sales]'" +
            "member [Product].x as 'sum({Product.members  * Gender.[All Gender]})' " +
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
        assertQueryReturns("with member [Product].x as 'sum({Product.members})' " +
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