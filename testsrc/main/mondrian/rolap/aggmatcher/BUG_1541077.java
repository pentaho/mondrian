/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2012 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap.aggmatcher;

import mondrian.olap.Result;

/**
 * Testcase for
 * <a href="http://jira.pentaho.com/browse/MONDRIAN-214">MONDRIAN-214</a>
 * (formerly SourceForge bug 1541077)
 * and a couple of other aggregate table ExplicitRecognizer conditions.
 *
 * @author Richard M. Emberson
 */
public class BUG_1541077 extends AggTableTestCase {

    private static final String BUG_1541077 = "BUG_1541077.csv";

    public BUG_1541077() {
        super();
    }
    public BUG_1541077(String name) {
        super(name);
    }

    public void testStoreCount() throws Exception {
        if (!isApplicable()) {
            return;
        }

        // get value without aggregates
        propSaver.set(propSaver.props.UseAggregates, false);

        String mdx =
            "select {[Measures].[Store Count]} on columns from Cheques";
        Result result = getCubeTestContext().executeQuery(mdx);
        Object v = result.getCell(new int[]{0}).getValue();

        propSaver.set(propSaver.props.UseAggregates, true);

        Result result1 = getCubeTestContext().executeQuery(mdx);
        Object v1 = result1.getCell(new int[]{0}).getValue();

        assertTrue(v.equals(v1));
    }

    public void testSalesCount() throws Exception {
        if (!isApplicable()) {
            return;
        }

        // get value without aggregates
        propSaver.set(propSaver.props.UseAggregates, false);

        String mdx =
            "select {[Measures].[Sales Count]} on columns from Cheques";
        Result result = getCubeTestContext().executeQuery(mdx);
        Object v = result.getCell(new int[]{0}).getValue();

        propSaver.set(propSaver.props.UseAggregates, true);

        Result result1 = getCubeTestContext().executeQuery(mdx);
        Object v1 = result1.getCell(new int[]{0}).getValue();

        assertTrue(v.equals(v1));
    }

    public void testTotalAmount() throws Exception {
        if (!isApplicable()) {
            return;
        }

        // get value without aggregates
        propSaver.set(propSaver.props.UseAggregates, false);

        String mdx =
            "select {[Measures].[Total Amount]} on columns from Cheques";
        Result result = getCubeTestContext().executeQuery(mdx);
        Object v = result.getCell(new int[]{0}).getValue();

        propSaver.set(propSaver.props.UseAggregates, true);

        Result result1 = getCubeTestContext().executeQuery(mdx);
        Object v1 = result1.getCell(new int[]{0}).getValue();

        assertTrue(v.equals(v1));
    }

    public void testBug1541077() throws Exception {
        if (!isApplicable()) {
            return;
        }

        // get value without aggregates
        propSaver.set(propSaver.props.UseAggregates, false);

        String mdx = "select {[Measures].[Avg Amount]} on columns from Cheques";

        Result result = getCubeTestContext().executeQuery(mdx);
        Object v = result.getCell(new int[]{0}).getFormattedValue();

        // get value with aggregates
        propSaver.set(propSaver.props.UseAggregates, true);

        Result result1 = getCubeTestContext().executeQuery(mdx);
        Object v1 = result1.getCell(new int[]{0}).getFormattedValue();

        assertTrue(v.equals(v1));
    }

    protected String getFileName() {
        return BUG_1541077;
    }

    protected String getCubeDescription() {
        return "<Cube name='Cheques'>\n"
               + "<Table name='cheques'>\n"
               + "<AggName name='agg_lp_xxx_cheques'>\n"
               + "<AggFactCount column='FACT_COUNT'/>\n"
               + "<AggForeignKey factColumn='store_id' aggColumn='store_id' />\n"
               + "<AggMeasure name='[Measures].[Avg Amount]'\n"
               + "   column='amount_AVG' />\n"

/*
            + "<AggLevel name='[Worker].[Worker]'\n"
            + "column='worker_worker_name'/>\n"
            + "<AggLevel name='[Discount Card].[Discount\n"
            + "Card Type]' column='discount_card_name'/>\n"
            + "<AggLevel name='[Department].[Department]'\n"
            + "column='department_department_name'/>\n"
            + "<AggLevel name='[Department].[Store]'\n"
            + "column='department_store_name'/>\n"
 */

               + "</AggName>\n"
               + "</Table>\n"

/*
            + "<DimensionUsage name='Year' source='Year'\n"
            + "   foreignKey='year_id'/>\n"
            + "<DimensionUsage name='Quarter'\n"
            + "   source='Quarter' foreignKey='quarter_id'/>\n"
            + "<DimensionUsage name='Month' source='Month'\n"
            + "   foreignKey='month_id'/>\n"
            + "<DimensionUsage name='Week' source='Week'\n"
            + "   foreignKey='week_id'/>\n"
            + "<DimensionUsage name='WeekDay'\n"
            + "   source='WeekDay' foreignKey='weekday_id'/>\n"
            + "<DimensionUsage name='Day' source='Day'\n"
            + "   foreignKey='day_id'/>\n"
            + "<DimensionUsage name='Hour' source='Hour'\n"
            + "   foreignKey='hour_id'/>\n"
            + "<DimensionUsage name='Worker' source='Worker'\n"
            + "   foreignKey='worker_id'/>\n"
            + "<DimensionUsage name='Discount Card'\n"
            + "   source='Discount Card' foreignKey='discount_card_id'/>\n"
            + "<DimensionUsage name='Department'\n"
            + "   source='Department' foreignKey='department_id'/>\n"
 */
/*
            + "<DimensionUsage name='Store' source='StoreX'\n"
            + "   foreignKey='store_id'/>\n"
            + "<DimensionUsage name='Product' source='ProductX'\n"
            + "   foreignKey='prod_id'/>\n"
 */
               + "<Dimension name='StoreX' foreignKey='store_id'>\n"
               + " <Hierarchy hasAll='true' primaryKey='store_id'>\n"
               + " <Table name='store_x'/>\n"
               + " <Level name='Store Value' column='value' uniqueMembers='true'/>\n"
               + " </Hierarchy>\n"
               + "</Dimension>\n"
               + "<Dimension name='ProductX' foreignKey='prod_id'>\n"
               + " <Hierarchy hasAll='true' primaryKey='prod_id'>\n"
               + " <Table name='product_x'/>\n"
               + " <Level name='Store Name' column='name' uniqueMembers='true'/>\n"
               + " </Hierarchy>\n"
               + "</Dimension>\n"

               + "<Measure name='Sales Count' \n"
               + "    column='prod_id' aggregator='count'\n"
               + "   formatString='#,###'/>\n"
               + "<Measure name='Store Count' \n"
               + "    column='store_id' aggregator='distinct-count'\n"
               + "   formatString='#,###'/>\n"
               + "<Measure name='Total Amount' \n"
               + "    column='amount' aggregator='sum'\n"
               + "   formatString='#,###'/>\n"
               + "<Measure name='Avg Amount' \n"
               + "    column='amount' aggregator='avg'\n"
               + "   formatString='00.0'/>\n"
               + "</Cube>";
    }
}

// End BUG_1541077.java
