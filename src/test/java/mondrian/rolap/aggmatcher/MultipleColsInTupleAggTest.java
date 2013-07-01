/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2013 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap.aggmatcher;

import mondrian.olap.Result;

/**
 * Testcase for levels that contain multiple columns and are
 * collapsed in the agg table.
 *
 * @author Will Gorman
 */
public class MultipleColsInTupleAggTest extends AggTableTestCase {

    public MultipleColsInTupleAggTest() {
        super();
    }

    public MultipleColsInTupleAggTest(String name) {
        super(name);
    }

    protected String getFileName() {
        return "multiple_cols_in_tuple_agg.csv";
    }

    public void testTotal() throws Exception {
        if (!isApplicable()) {
            return;
        }

        // get value without aggregates
        propSaver.set(propSaver.props.UseAggregates, false);
        propSaver.set(propSaver.props.ReadAggregates, false);

        String mdx =
            "select {[Measures].[Total]} on columns from [Fact]";
        Result result = getCubeTestContext().executeQuery(mdx);
        Object v = result.getCell(new int[]{0}).getValue();

        String mdx2 =
            "select {[Measures].[Total]} on columns from [Fact] where "
            + "{[Product].[Products].[Cat One].[Prod Cat One].[One]}";
        Result aresult = getCubeTestContext().executeQuery(mdx2);
        Object av = aresult.getCell(new int[]{0}).getValue();

        // unless there is a way to flush the cache,
        // I'm skeptical about these results
        propSaver.set(propSaver.props.UseAggregates, true);
        propSaver.set(propSaver.props.ReadAggregates, false);

        Result result1 = getCubeTestContext().executeQuery(mdx);
        Object v1 = result1.getCell(new int[]{0}).getValue();

        assertTrue(v.equals(v1));

        Result aresult2 = getCubeTestContext().executeQuery(mdx2);
        Object av1 = aresult2.getCell(new int[]{0}).getValue();

        assertTrue(av.equals(av1));
    }

    public void testTupleSelection() throws Exception {
        if (!isApplicable()) {
            return;
        }

        String mdx =
            "select "
            + "{[Measures].[Total]} on columns, "
            + "non empty CrossJoin({[Product].[Cat One].[Prod Cat One]},"
            + "{[Store].[Store].[All Stores]}) on rows "
            + "from [Fact]";

        getCubeTestContext().assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Total]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[Cat One].[Prod Cat One],"
            + " [Store].[Store].[All Stores]}\n"
            + "Row #0: 15\n");
    }

    public void testChildSelection() throws Exception {
        if (!isApplicable()) {
            return;
        }

        String mdx = "select {[Measures].[Total]} on columns, "
            + "non empty [Product].[Cat One].Children on rows from [Fact]";
        getCubeTestContext().assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Total]}\n"
            + "Axis #2:\n"
            + "{[Product].[Products].[Cat One].[Prod Cat Two]}\n"
            + "{[Product].[Products].[Cat One].[Prod Cat One]}\n"
            + "Row #0: 18\n"
            + "Row #1: 15\n");
    }

    protected String getCubeDescription() {
        return "<Cube name='Fact'>\n"
           + "<Table name='fact'>\n"
           + " <AggName name='test_lp_xxx_fact'>\n"
           + "  <AggFactCount column='fact_count'/>\n"
           + "  <AggMeasure column='amount' name='[Measures].[Total]'/>\n"
           + "  <AggLevel column='category' name='[Product].[Category]'/>\n"
           + "  <AggLevel column='product_category' "
           + "            name='[Product].[Product Category]'/>\n"
           + " </AggName>\n"
           + "</Table>"
           + "<Dimension name='Store' foreignKey='store_id'>\n"
           + " <Hierarchy hasAll='true' primaryKey='store_id'>\n"
           + "  <Table name='store_csv'/>\n"
           + "  <Level name='Store Value' column='value' "
           + "         uniqueMembers='true'/>\n"
           + " </Hierarchy>\n"
           + "</Dimension>\n"
           + "<Dimension name='Product' foreignKey='prod_id'>\n"
           + " <Hierarchy hasAll='true' name='Products' primaryKey='prod_id' "
           + "primaryKeyTable='product_csv'>\n"
           + " <Join leftKey='prod_cat' rightAlias='product_cat' "
           + "rightKey='prod_cat'>\n"
           + "  <Table name='product_csv'/>\n"
           + "  <Join leftKey='cat' rightKey='cat'>\n"
           + "   <Table name='product_cat'/>\n"
           + "   <Table name='cat'/>\n"
           + "  </Join>"
           + " </Join>\n"
           + " <Level name='Category' table='cat' column='cat' "
           + "ordinalColumn='ord' captionColumn='cap' nameColumn='name3' "
           + "uniqueMembers='false' type='Numeric'/>\n"
           + " <Level name='Product Category' table='product_cat' "
           + "column='name2' ordinalColumn='ord' captionColumn='cap' "
           + "uniqueMembers='false'/>\n"
           + " <Level name='Product Name' table='product_csv' column='name1' "
           + "uniqueMembers='true'/>\n"
           + " </Hierarchy>\n"
           + "</Dimension>\n"
           + "<Measure name='Total' \n"
           + "    column='amount' aggregator='sum'\n"
           + "   formatString='#,###'/>\n"
           + "</Cube>";
    }
}
// End MultipleColsInTupleAggTest.java
