/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2020 Hitachi Vantara..  All rights reserved.
*/

package mondrian.rolap.aggmatcher;

import mondrian.olap.MondrianProperties;
import mondrian.test.TestContext;

/**
 * Test case for non-collapsed levels in agg tables, based on the "Species"
 * schema.
 */
public class SpeciesNonCollapsedAggTest extends AggTableTestCase {

    public static final String ANIMAL_SCHEMA =
        "<?xml version='1.0'?>\n"
        + "<Schema name='Testmart'>\n"
        + "  <Dimension name='Animal'>\n"
        + "    <Hierarchy name='Animals' hasAll='true' allMemberName='All Animals' primaryKey='SPECIES_ID' primaryKeyTable='DIM_SPECIES'>\n"
        + "      <Join leftKey='GENUS_ID' rightAlias='DIM_GENUS' rightKey='GENUS_ID'>\n"
        + "        <Table name='DIM_SPECIES' />\n"
        + "        <Join leftKey='FAMILY_ID' rightKey='FAMILY_ID'>\n"
        + "          <Table name='DIM_GENUS' />\n"
        + "          <Table name='DIM_FAMILY' />\n"
        + "        </Join>\n"
        + "      </Join>\n"
        + "      <Level name='Family' table='DIM_FAMILY' column='FAMILY_ID' nameColumn='FAMILY_NAME' uniqueMembers='true' type='Numeric' approxRowCount='2' />\n"
        + "      <Level name='Genus' table='DIM_GENUS' column='GENUS_ID' nameColumn='GENUS_NAME' uniqueMembers='true' type='Numeric' approxRowCount='4' />\n"
        + "      <Level name='Species' table='DIM_SPECIES' column='SPECIES_ID' nameColumn='SPECIES_NAME' uniqueMembers='true' type='Numeric' approxRowCount='8' />\n"
        + "    </Hierarchy>\n"
        + "  </Dimension>\n"
        + "  <Cube name='Test' defaultMeasure='Population'>\n"
        + "    <Table name='species_mart'>\n" // See MONDRIAN-2237 - Table name needs to be lower case for embedded Windows MySQL integration testing
        + "      <AggName name='AGG_SPECIES_MART'>\n"
        + "        <AggFactCount column='FACT_COUNT' />\n"
        + "        <AggMeasure name='Measures.[Population]' column='POPULATION' />\n"
        + "        <AggLevel name='[Animal.Animals].[Genus]' column='GEN_ID' collapsed='false' />\n"
        + "      </AggName>\n"
        + "    </Table>\n"
        + "    <DimensionUsage name='Animal' source='Animal' foreignKey='SPECIES_ID'/>\n"
        + "    <Measure name='Population' column='POPULATION' aggregator='sum'/>\n"
        + "  </Cube>\n"
        + "  <Role name='Test role'>\n"
        + "    <SchemaGrant access='none'>\n"
        + "      <CubeGrant cube='Test' access='all'>\n"
        + "        <HierarchyGrant hierarchy='[Animal.Animals]' access='custom' rollupPolicy='partial'>\n"
        + "          <MemberGrant member='[Animal.Animals].[Family].[Loricariidae]' access='all'/>\n"
        + "          <MemberGrant member='[Animal.Animals].[Family].[Cichlidae]' access='all'/>\n"
        + "          <MemberGrant member='[Animal.Animals].[Family].[Cyprinidae]' access='none'/>\n"
        + "        </HierarchyGrant>\n"
        + "      </CubeGrant>\n"
        + "    </SchemaGrant>\n"
        + "  </Role>\n"
        + "</Schema>";

    public SpeciesNonCollapsedAggTest() {
        super();
    }

    @SuppressWarnings("UnusedDeclaration")
    public SpeciesNonCollapsedAggTest(String name) {
        super(name);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        final MondrianProperties props = MondrianProperties.instance();
        propSaver.set(props.UseAggregates, true);
        propSaver.set(props.ReadAggregates, true);
        super.getConnection().getCacheControl(null).flushSchemaCache();
    }

    protected String getFileName() {
        return "species_schema.csv";
    }

    @Override
    protected TestContext createTestContext() {
        return TestContext.instance()
            .withSchema(ANIMAL_SCHEMA);
    }

    /**
     * Test case for bug
     * <a href="http://jira.pentaho.com/browse/MONDRIAN-1105">MONDRIAN-1105,
     * "AggLevel column attribute not used properly in all cases"</a>.
     */
    public void testBugMondrian1105() {
        if (!isApplicable()) {
            return;
        }

        // If agg table is not used, cell values will be very different.
        getTestContext().assertQueryReturns(
            "SELECT \n"
            + " { [Measures].[Population] } ON COLUMNS,\n"
            + " { [Animal.Animals].[Family].Members } ON ROWS\n"
            + "FROM [Test]\n",
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Population]}\n"
            + "Axis #2:\n"
            + "{[Animal.Animals].[Loricariidae]}\n"
            + "{[Animal.Animals].[Cichlidae]}\n"
            + "{[Animal.Animals].[Cyprinidae]}\n"
            + "Row #0: 666\n"
            + "Row #1: 579\n"
            + "Row #2: 479\n");
    }
}

// End SpeciesNonCollapsedAggTest.java
