/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2002-2017 Hitachi Vantara..  All rights reserved.
*/
package mondrian.rolap.aggmatcher;

import mondrian.olap.MondrianProperties;
import mondrian.test.TestContext;

/**
 * Testcase for non-collapsed levels in agg tables.
 *
 * @author Luc Boudreau
 */
public class NonCollapsedAggTest extends AggTableTestCase {

    private static final String CUBE_1 =
        "<Cube name=\"foo\">\n"
        + "    <Table name=\"foo_fact\">\n"
        + "        <AggName name=\"agg_tenant\">\n"
        + "            <AggFactCount column=\"fact_count\"/>\n"
        + "            <AggMeasure name=\"[Measures].[Unit Sales]\" column=\"unit_sales\"/>\n"
        + "            <AggLevel name=\"[dimension.tenant].[tenant]\"\n"
        + "                column=\"tenant_id\" collapsed=\"false\"/>\n"
        + "        </AggName>\n"
        + "        <AggName name=\"agg_line_class\">\n"
        + "            <AggFactCount column=\"fact_count\"/>\n"
        + "            <AggMeasure name=\"[Measures].[Unit Sales]\" column=\"unit_sales\"/>\n"
        + "            <AggLevel name=\"[dimension.distributor].[line class]\"\n"
        + "                column=\"line_class_id\" collapsed=\"false\"/>\n"
        + "        </AggName>\n"
        + "        <AggName name=\"agg_line_class\">\n"
        + "            <AggFactCount column=\"fact_count\"/>\n"
        + "            <AggMeasure name=\"[Measures].[Unit Sales]\" column=\"unit_sales\"/>\n"
        + "            <AggLevel name=\"[dimension.network].[line class]\"\n"
        + "                column=\"line_class_id\" collapsed=\"false\"/>\n"
        + "        </AggName>\n"
        + "    </Table>\n"
        + "    <Dimension name=\"dimension\" foreignKey=\"line_id\">\n"
        + "        <Hierarchy name=\"tenant\" hasAll=\"true\" allMemberName=\"All tenants\"\n"
        + "            primaryKey=\"line_id\" primaryKeyTable=\"line\">\n"
        + "            <Join leftKey=\"line_id\" rightKey=\"line_id\"\n"
        + "                rightAlias=\"line_tenant\">\n"
        + "                <Table name=\"line\"/>\n"
        + "                <Join leftKey=\"tenant_id\" rightKey=\"tenant_id\">\n"
        + "                    <Table name=\"line_tenant\"/>\n"
        + "                    <Table name=\"tenant\"/>\n"
        + "                </Join>\n"
        + "            </Join>\n"
        + "            <Level name=\"tenant\" table=\"tenant\" column=\"tenant_id\" nameColumn=\"tenant_name\" uniqueMembers=\"true\"/>\n"
        + "            <Level name=\"line\" table=\"line\" column=\"line_id\" nameColumn=\"line_name\"/>\n"
        + "        </Hierarchy>\n"
        + "        <Hierarchy name=\"distributor\" hasAll=\"true\" allMemberName=\"All distributors\"\n"
        + "            primaryKey=\"line_id\" primaryKeyTable=\"line\">\n"
        + "            <Join leftKey=\"line_id\" rightKey=\"line_id\" rightAlias=\"line_line_class\">\n"
        + "                <Table name=\"line\"/>\n"
        + "                <Join leftKey=\"line_class_id\" rightKey=\"line_class_id\" rightAlias=\"line_class\">\n"
        + "                    <Table name=\"line_line_class\"/>\n"
        + "                    <Join leftKey=\"line_class_id\" rightKey=\"line_class_id\" rightAlias=\"line_class_distributor\">\n"
        + "                        <Table name=\"line_class\"/>\n"
        + "                        <Join leftKey=\"distributor_id\" rightKey=\"distributor_id\">\n"
        + "                            <Table name=\"line_class_distributor\"/>\n"
        + "                            <Table name=\"distributor\"/>\n"
        + "                        </Join>\n"
        + "                    </Join>\n"
        + "                </Join>\n"
        + "            </Join>\n"
        + "            <Level name=\"distributor\" table=\"distributor\" column=\"distributor_id\" nameColumn=\"distributor_name\"/>\n"
        + "            <Level name=\"line class\" table=\"line_class\" column=\"line_class_id\" nameColumn=\"line_class_name\" uniqueMembers=\"true\"/>\n"
        + "            <Level name=\"line\" table=\"line\" column=\"line_id\" nameColumn=\"line_name\"/>\n"
        + "        </Hierarchy>\n"
        + "        <Hierarchy name=\"network\" hasAll=\"true\" allMemberName=\"All networks\"\n"
        + "            primaryKey=\"line_id\" primaryKeyTable=\"line\">\n"
        + "            <Join leftKey=\"line_id\" rightKey=\"line_id\" rightAlias=\"line_line_class\">\n"
        + "                <Table name=\"line\"/>\n"
        + "                <Join leftKey=\"line_class_id\" rightKey=\"line_class_id\" rightAlias=\"line_class\">\n"
        + "                    <Table name=\"line_line_class\"/>\n"
        + "                    <Join leftKey=\"line_class_id\" rightKey=\"line_class_id\" rightAlias=\"line_class_network\">\n"
        + "                        <Table name=\"line_class\"/>\n"
        + "                        <Join leftKey=\"network_id\" rightKey=\"network_id\">\n"
        + "                            <Table name=\"line_class_network\"/>\n"
        + "                            <Table name=\"network\"/>\n"
        + "                        </Join>\n"
        + "                    </Join>\n"
        + "                </Join>\n"
        + "            </Join>\n"
        + "            <Level name=\"network\" table=\"network\" column=\"network_id\" nameColumn=\"network_name\"/>\n"
        + "            <Level name=\"line class\" table=\"line_class\" column=\"line_class_id\" nameColumn=\"line_class_name\" uniqueMembers=\"true\"/>\n"
        + "            <Level name=\"line\" table=\"line\" column=\"line_id\" nameColumn=\"line_name\"/>\n"
        + "        </Hierarchy>\n"
        + " </Dimension>\n"
        + "   <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\" formatString=\"Standard\" />\n"
        + "</Cube>\n"
        + "<Cube name=\"foo2\">\n"
        + "    <Table name=\"foo_fact\">\n"
        + "    </Table>\n"
        + "    <Dimension name=\"dimension\" foreignKey=\"line_id\">\n"
        + "        <Hierarchy name=\"tenant\" hasAll=\"true\" allMemberName=\"All tenants\"\n"
        + "            primaryKey=\"line_id\" primaryKeyTable=\"line\">\n"
        + "            <Join leftKey=\"line_id\" rightKey=\"line_id\"\n"
        + "                rightAlias=\"line_tenant\">\n"
        + "                <Table name=\"line\"/>\n"
        + "                <Join leftKey=\"tenant_id\" rightKey=\"tenant_id\">\n"
        + "                    <Table name=\"line_tenant\"/>\n"
        + "                    <Table name=\"tenant\"/>\n"
        + "                </Join>\n"
        + "            </Join>\n"
        + "            <Level name=\"tenant\" table=\"tenant\" column=\"tenant_id\" nameColumn=\"tenant_name\" uniqueMembers=\"true\"/>\n"
        + "            <Level name=\"line\" table=\"line\" column=\"line_id\" nameColumn=\"line_name\"/>\n"
        + "        </Hierarchy>\n"
        + "        <Hierarchy name=\"distributor\" hasAll=\"true\" allMemberName=\"All distributors\"\n"
        + "            primaryKey=\"line_id\" primaryKeyTable=\"line\">\n"
        + "            <Join leftKey=\"line_id\" rightKey=\"line_id\" rightAlias=\"line_line_class\">\n"
        + "                <Table name=\"line\"/>\n"
        + "                <Join leftKey=\"line_class_id\" rightKey=\"line_class_id\" rightAlias=\"line_class\">\n"
        + "                    <Table name=\"line_line_class\"/>\n"
        + "                    <Join leftKey=\"line_class_id\" rightKey=\"line_class_id\" rightAlias=\"line_class_distributor\">\n"
        + "                        <Table name=\"line_class\"/>\n"
        + "                        <Join leftKey=\"distributor_id\" rightKey=\"distributor_id\">\n"
        + "                            <Table name=\"line_class_distributor\"/>\n"
        + "                            <Table name=\"distributor\"/>\n"
        + "                        </Join>\n"
        + "                    </Join>\n"
        + "                </Join>\n"
        + "            </Join>\n"
        + "            <Level name=\"distributor\" table=\"distributor\" column=\"distributor_id\" nameColumn=\"distributor_name\"/>\n"
        + "            <Level name=\"line class\" table=\"line_class\" column=\"line_class_id\" nameColumn=\"line_class_name\" uniqueMembers=\"true\"/>\n"
        + "            <Level name=\"line\" table=\"line\" column=\"line_id\" nameColumn=\"line_name\"/>\n"
        + "        </Hierarchy>\n"
        + "        <Hierarchy name=\"network\" hasAll=\"true\" allMemberName=\"All networks\"\n"
        + "            primaryKey=\"line_id\" primaryKeyTable=\"line\">\n"
        + "            <Join leftKey=\"line_id\" rightKey=\"line_id\" rightAlias=\"line_line_class\">\n"
        + "                <Table name=\"line\"/>\n"
        + "                <Join leftKey=\"line_class_id\" rightKey=\"line_class_id\" rightAlias=\"line_class\">\n"
        + "                    <Table name=\"line_line_class\"/>\n"
        + "                    <Join leftKey=\"line_class_id\" rightKey=\"line_class_id\" rightAlias=\"line_class_network\">\n"
        + "                        <Table name=\"line_class\"/>\n"
        + "                        <Join leftKey=\"network_id\" rightKey=\"network_id\">\n"
        + "                            <Table name=\"line_class_network\"/>\n"
        + "                            <Table name=\"network\"/>\n"
        + "                        </Join>\n"
        + "                    </Join>\n"
        + "                </Join>\n"
        + "            </Join>\n"
        + "            <Level name=\"network\" table=\"network\" column=\"network_id\" nameColumn=\"network_name\"/>\n"
        + "            <Level name=\"line class\" table=\"line_class\" column=\"line_class_id\" nameColumn=\"line_class_name\" uniqueMembers=\"true\"/>\n"
        + "            <Level name=\"line\" table=\"line\" column=\"line_id\" nameColumn=\"line_name\"/>\n"
        + "        </Hierarchy>\n"
        + " </Dimension>\n"
        + "   <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\" formatString=\"Standard\" />\n"
        + "</Cube>\n";
    private static final String SSAS_COMPAT_CUBE = "<Cube name=\"testSsas\">\n"
            + "    <Table name=\"foo_fact\">\n"
            + "        <AggName name=\"agg_tenant\">\n"
            + "            <AggFactCount column=\"fact_count\"/>\n"
            + "            <AggMeasure name=\"[Measures].[Unit Sales]\" column=\"unit_sales\"/>\n"
            + "            <AggLevel name=\"[dimension].[tenant].[tenant]\"\n"
            + "                column=\"tenant_id\" collapsed=\"false\"/>\n"
            + "        </AggName>\n"
            + "        <AggName name=\"agg_line_class\">\n"
            + "            <AggFactCount column=\"fact_count\"/>\n"
            + "            <AggMeasure name=\"[Measures].[Unit Sales]\" column=\"unit_sales\"/>\n"
            + "            <AggLevel name=\"[dimension].[distributor].[line class]\"\n"
            + "                column=\"line_class_id\" collapsed=\"false\"/>\n"
            + "        </AggName>\n"
            + "        <AggName name=\"agg_line_class\">\n"
            + "            <AggFactCount column=\"fact_count\"/>\n"
            + "            <AggMeasure name=\"[Measures].[Unit Sales]\" column=\"unit_sales\"/>\n"
            + "            <AggLevel name=\"[dimension].[network].[line class]\"\n"
            + "                column=\"line_class_id\" collapsed=\"false\"/>\n"
            + "        </AggName>\n"
            + "    </Table>\n"
            + "    <Dimension name=\"dimension\" foreignKey=\"line_id\">\n"
            + "        <Hierarchy name=\"tenant\" hasAll=\"true\" allMemberName=\"All tenants\"\n"
            + "            primaryKey=\"line_id\" primaryKeyTable=\"line\">\n"
            + "            <Join leftKey=\"line_id\" rightKey=\"line_id\"\n"
            + "                rightAlias=\"line_tenant\">\n"
            + "                <Table name=\"line\"/>\n"
            + "                <Join leftKey=\"tenant_id\" rightKey=\"tenant_id\">\n"
            + "                    <Table name=\"line_tenant\"/>\n"
            + "                    <Table name=\"tenant\"/>\n"
            + "                </Join>\n"
            + "            </Join>\n"
            + "            <Level name=\"tenant\" table=\"tenant\" column=\"tenant_id\" nameColumn=\"tenant_name\" uniqueMembers=\"true\"/>\n"
            + "            <Level name=\"line\" table=\"line\" column=\"line_id\" nameColumn=\"line_name\"/>\n"
            + "        </Hierarchy>\n"
            + "        <Hierarchy name=\"distributor\" hasAll=\"true\" allMemberName=\"All distributors\"\n"
            + "            primaryKey=\"line_id\" primaryKeyTable=\"line\">\n"
            + "            <Join leftKey=\"line_id\" rightKey=\"line_id\" rightAlias=\"line_line_class\">\n"
            + "                <Table name=\"line\"/>\n"
            + "                <Join leftKey=\"line_class_id\" rightKey=\"line_class_id\" rightAlias=\"line_class\">\n"
            + "                    <Table name=\"line_line_class\"/>\n"
            + "                    <Join leftKey=\"line_class_id\" rightKey=\"line_class_id\" rightAlias=\"line_class_distributor\">\n"
            + "                        <Table name=\"line_class\"/>\n"
            + "                        <Join leftKey=\"distributor_id\" rightKey=\"distributor_id\">\n"
            + "                            <Table name=\"line_class_distributor\"/>\n"
            + "                            <Table name=\"distributor\"/>\n"
            + "                        </Join>\n"
            + "                    </Join>\n"
            + "                </Join>\n"
            + "            </Join>\n"
            + "            <Level name=\"distributor\" table=\"distributor\" column=\"distributor_id\" nameColumn=\"distributor_name\"/>\n"
            + "            <Level name=\"line class\" table=\"line_class\" column=\"line_class_id\" nameColumn=\"line_class_name\" uniqueMembers=\"true\"/>\n"
            + "            <Level name=\"line\" table=\"line\" column=\"line_id\" nameColumn=\"line_name\"/>\n"
            + "        </Hierarchy>\n"
            + "        <Hierarchy name=\"network\" hasAll=\"true\" allMemberName=\"All networks\"\n"
            + "            primaryKey=\"line_id\" primaryKeyTable=\"line\">\n"
            + "            <Join leftKey=\"line_id\" rightKey=\"line_id\" rightAlias=\"line_line_class\">\n"
            + "                <Table name=\"line\"/>\n"
            + "                <Join leftKey=\"line_class_id\" rightKey=\"line_class_id\" rightAlias=\"line_class\">\n"
            + "                    <Table name=\"line_line_class\"/>\n"
            + "                    <Join leftKey=\"line_class_id\" rightKey=\"line_class_id\" rightAlias=\"line_class_network\">\n"
            + "                        <Table name=\"line_class\"/>\n"
            + "                        <Join leftKey=\"network_id\" rightKey=\"network_id\">\n"
            + "                            <Table name=\"line_class_network\"/>\n"
            + "                            <Table name=\"network\"/>\n"
            + "                        </Join>\n"
            + "                    </Join>\n"
            + "                </Join>\n"
            + "            </Join>\n"
            + "            <Level name=\"network\" table=\"network\" column=\"network_id\" nameColumn=\"network_name\"/>\n"
            + "            <Level name=\"line class\" table=\"line_class\" column=\"line_class_id\" nameColumn=\"line_class_name\" uniqueMembers=\"true\"/>\n"
            + "            <Level name=\"line\" table=\"line\" column=\"line_id\" nameColumn=\"line_name\"/>\n"
            + "        </Hierarchy>\n"
            + " </Dimension>\n"
            + "   <Measure name=\"Unit Sales\" column=\"unit_sales\" aggregator=\"sum\" formatString=\"Standard\" />\n"
            + "</Cube>\n";
    public NonCollapsedAggTest() {
        super();
    }

    public NonCollapsedAggTest(String name) {
        super(name);
    }

    protected String getFileName() {
        return "non_collapsed_agg_test.csv";
    }

    @Override
    protected String getCubeDescription() {
        return CUBE_1;
    }

    @Override
    public TestContext getTestContext() {
        TestContext.instance().flushSchemaCache();
        return TestContext.instance().create(
            null, getCubeDescription(), null, null, null, null);
    }

    public void testSingleJoin() throws Exception {
        if (!isApplicable()) {
            return;
        }

        final String mdx =
            "select {[Measures].[Unit Sales]} on columns, {[dimension.tenant].[tenant].Members} on rows from [foo]";

        final TestContext context = getTestContext();

        // We expect the correct cell value + 1 if the agg table is used.
        context.assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[dimension.tenant].[tenant one]}\n"
            + "{[dimension.tenant].[tenant two]}\n"
            + "Row #0: 31\n"
            + "Row #1: 121\n");
    }

    public void testComplexJoin() throws Exception {
        if (!isApplicable()) {
            return;
        }

        final String mdx =
            "select {[Measures].[Unit Sales]} on columns, {[dimension.distributor].[line class].Members} on rows from [foo]";

        final TestContext context = getTestContext();

        // We expect the correct cell value + 1 if the agg table is used.
        context.assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[dimension.distributor].[distributor one].[line class one]}\n"
            + "{[dimension.distributor].[distributor two].[line class two]}\n"
            + "Row #0: 31\n"
            + "Row #1: 121\n");

        final String mdx2 =
            "select {[Measures].[Unit Sales]} on columns, {[dimension.network].[line class].Members} on rows from [foo]";
        // We expect the correct cell value + 1 if the agg table is used.
        context.assertQueryReturns(
            mdx2,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[dimension.network].[network one].[line class one]}\n"
            + "{[dimension.network].[network two].[line class two]}\n"
            + "Row #0: 31\n"
            + "Row #1: 121\n");
    }

    public void testComplexJoinDefaultRecognizer() throws Exception {
        if (!isApplicable()) {
            return;
        }

        final TestContext context = getTestContext();

        // We expect the correct cell value + 2 if the agg table is used.
        final String mdx =
            "select {[Measures].[Unit Sales]} on columns, {[dimension.distributor].[line class].Members} on rows from [foo2]";
        context.assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[dimension.distributor].[distributor one].[line class one]}\n"
            + "{[dimension.distributor].[distributor two].[line class two]}\n"
            + "Row #0: 32\n"
            + "Row #1: 122\n");

        final String mdx2 =
            "select {[Measures].[Unit Sales]} on columns, {[dimension.network].[line class].Members} on rows from [foo2]";
        // We expect the correct cell value + 2 if the agg table is used.
        context.assertQueryReturns(
            mdx2,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[dimension.network].[network one].[line class one]}\n"
            + "{[dimension.network].[network two].[line class two]}\n"
            + "Row #0: 32\n"
            + "Row #1: 122\n");
    }

    public void testSsasCompatNamingInAgg() throws Exception {
        // MONDRIAN-1085
        if (!MondrianProperties.instance().SsasCompatibleNaming.get()) {
            return;
        }

        final TestContext testContext = TestContext.instance().create(
            null, SSAS_COMPAT_CUBE, null, null, null, null);
        final String mdx =
                "select {[Measures].[Unit Sales]} on columns, {[dimension].[tenant].[tenant].Members} on rows from [testSsas]";
        testContext.assertQueryReturns(
            mdx,
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Unit Sales]}\n"
            + "Axis #2:\n"
            + "{[dimension].[tenant].[tenant one]}\n"
            + "{[dimension].[tenant].[tenant two]}\n"
            + "Row #0: 31\n"
            + "Row #1: 121\n");
    }

    /**
     * Test case for cast exception on min/max of an integer measure
     */
    public void testMondrian1325() {
        final String query1 =
            "SELECT\n"
            + "{ Measures.[Bogus Number]} on 0,\n"
            + "non empty Descendants([Time].[Year].Members, Time.Month, SELF) on 1\n"
            + "FROM [Sales]\n";

        final String query2 =
            "SELECT\n"
            + "{ Measures.[Bogus Number]} on 0,\n"
            + "non empty Descendants([Time].[Year].Members, Time.Month, SELF_AND_BEFORE) on 1\n"
            + "FROM [Sales]";

        TestContext context =
            getTestContext().createSubstitutingCube(
                "Sales",
                null,
                "<Measure name=\"Bogus Number\" column=\"promotion_id\" datatype=\"Numeric\" aggregator=\"max\" visible=\"true\"/>",
                null,
                null);

        context.executeQuery(query1);
        context.executeQuery(query2);
    }

}
// End NonCollapsedAggTest.java
