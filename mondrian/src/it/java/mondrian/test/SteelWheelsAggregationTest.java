/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2003-2005 Julian Hyde
// Copyright (C) 2005-2017 Hitachi Vantara
// All Rights Reserved.
*/
package mondrian.test;

/**
 * @author Andrey Khayrutdinov
 */
public class SteelWheelsAggregationTest extends SteelWheelsTestCase {

    private static final String QUERY = ""
        + "WITH\n"
        + "SET [*NATIVE_CJ_SET_WITH_SLICER] AS 'FILTER([*BASE_MEMBERS__Customer_DimUsage.Customers Hierarchy_], NOT ISEMPTY ([Measures].[Price Each]))'\n"
        + "SET [*NATIVE_CJ_SET] AS '[*NATIVE_CJ_SET_WITH_SLICER]'\n"
        + "SET [*SORTED_ROW_AXIS] AS 'ORDER([*CJ_ROW_AXIS],[Customer_DimUsage.Customers Hierarchy].CURRENTMEMBER.ORDERKEY,"
        + "BASC,ANCESTOR([Customer_DimUsage.Customers Hierarchy].CURRENTMEMBER,[Customer_DimUsage.Customers Hierarchy].[Address]).ORDERKEY,BASC)'\n"
        + "SET [*BASE_MEMBERS__Measures_] AS '{[Measures].[Price Each]}'\n"
        + "SET [*BASE_MEMBERS__Customer_DimUsage.Customers Hierarchy_] AS '[Customer_DimUsage.Customers Hierarchy].[Name].MEMBERS'\n"
        + "SET [*CJ_ROW_AXIS] AS 'GENERATE([*NATIVE_CJ_SET], {([Customer_DimUsage.Customers Hierarchy].CURRENTMEMBER)})'\n"
        + "SELECT\n"
        + "[*BASE_MEMBERS__Measures_] ON COLUMNS\n"
        + ",[*SORTED_ROW_AXIS] ON ROWS\n"
        + "FROM [Customers Cube]\n";

    private static final String EXPECTED = ""
        + "Axis #0:\n"
        + "{}\n"
        + "Axis #1:\n"
        + "{[Measures].[Price Each]}\n"
        + "Axis #2:\n"
        + "{[Customer_DimUsage.Customers Hierarchy].[1 rue Alsace-Lorraine].[Roulet]}\n"
        + "Row #0: 1,701.95\n";

    private PropertySaver propertySaver;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        propertySaver = new PropertySaver();
        propertySaver.set(propertySaver.properties.UseAggregates, true);
        propertySaver.set(propertySaver.properties.ReadAggregates, true);
    }

    @Override
    public void tearDown() throws Exception {
        propertySaver.reset();
        super.tearDown();
    }

    private String getSchemaWith(String roles) {
        return String.format
          (""
                + "<Schema name=\"SteelWheels\" description=\"1 admin role, 1 user role. For testing MemberGrant with caching in 5.1.2\"> \n"
                + "  <Dimension type=\"StandardDimension\" visible=\"true\" highCardinality=\"false\" name=\"Customers Dimension\">\n"
                + "    <Hierarchy name=\"Customers Hierarchy\" visible=\"true\" hasAll=\"true\" primaryKey=\"CUSTOMERNUMBER\" caption=\"Customer Hierarchy\">\n"
                + "      <Table name=\"customer_w_ter\">\n"
                + "      </Table>\n"
                + "      <Level name=\"Address\" visible=\"true\" column=\"ADDRESSLINE1\" type=\"String\" uniqueMembers=\"false\" levelType=\"Regular\" hideMemberIf=\"Never\" "
                +        "caption=\"Address Line 1\">\n"
                + "      </Level>\n"
                + "      <Level name=\"Name\" visible=\"true\" column=\"CONTACTLASTNAME\" type=\"String\" uniqueMembers=\"false\" levelType=\"Regular\" hideMemberIf=\"Never\" "
                + "      caption=\"Contact Last Name\">\n"
                + "      </Level>\n"
                + "    </Hierarchy>\n"
                + "  </Dimension>\n"
                + " <Cube name=\"Customers Cube\" visible=\"true\" cache=\"true\" enabled=\"true\"> \n"
                + "     <Table name=\"orderfact\"> \n"
                + "     </Table> \n"
                + "     <DimensionUsage source=\"Customers Dimension\" name=\"Customer_DimUsage\" visible=\"true\" foreignKey=\"CUSTOMERNUMBER\" highCardinality=\"false\"> \n"
                + "     </DimensionUsage> \n"
                + "     <Measure name=\"Price Each\" column=\"PRICEEACH\" aggregator=\"sum\" visible=\"true\"> \n"
                + "     </Measure> \n"
                + "     <Measure name=\"Total Price\" column=\"TOTALPRICE\" aggregator=\"sum\" visible=\"true\"> \n"
                + "     </Measure> \n"
                + " </Cube> \n"
                + "%s"
                + "</Schema>\n", roles);
    }

    public void testWithAggregation() throws Exception {
        final String schema = getSchemaWith
            (""
            + " <Role name=\"Power User\"> \n"
            + "     <SchemaGrant access=\"none\"> \n"
            + "         <CubeGrant cube=\"Customers Cube\" access=\"all\"> \n"
            + "             <DimensionGrant dimension=\"Measures\" access=\"all\"> \n"
            + "             </DimensionGrant>\n"
            + "             <HierarchyGrant hierarchy=\"[Customer_DimUsage.Customers Hierarchy]\" topLevel=\"[Customer_DimUsage.Customers Hierarchy].[Name]\" "
            + "                 rollupPolicy=\"partial\" access=\"custom\"> \n"
            + "                 <MemberGrant member=\"[Customer_DimUsage.Customers Hierarchy].[1 rue Alsace-Lorraine]\" access=\"none\"/> \n"
            + "                 <MemberGrant member=\"[Customer_DimUsage.Customers Hierarchy].[1 rue Alsace-Lorraine].[Roulet]\" access=\"all\" /> \n"
            + "             </HierarchyGrant> \n"
            + "         </CubeGrant> \n"
            + "     </SchemaGrant> \n"
            + " </Role>\n");

        getTestContext()
            .withSchema(schema)
            .withRole("Power User")
            .assertQueryReturns(QUERY, EXPECTED);
    }

    public void testWithAggregationNoRestrictionsOnTopLevel() throws Exception {
        final String schema = getSchemaWith
          (""
          + " <Role name=\"Power User\"> \n"
          + "     <SchemaGrant access=\"none\"> \n"
          + "         <CubeGrant cube=\"Customers Cube\" access=\"all\"> \n"
          + "             <DimensionGrant dimension=\"Measures\" access=\"all\"> \n"
          + "             </DimensionGrant>\n"
          + "             <HierarchyGrant hierarchy=\"[Customer_DimUsage.Customers Hierarchy]\" topLevel=\"[Customer_DimUsage.Customers Hierarchy].[Name]\" "
          + "                                                          rollupPolicy=\"Partial\" access=\"custom\"> \n"
          + "                 <MemberGrant member=\"[Customer_DimUsage.Customers Hierarchy].[1 rue Alsace-Lorraine].[Roulet]\" access=\"all\" /> \n"
          + "             </HierarchyGrant> \n"
          + "         </CubeGrant> \n"
          + "     </SchemaGrant> \n"
          + " </Role>\n");

        getTestContext()
          .withSchema(schema)
          .withRole("Power User")
          .assertQueryReturns(QUERY, EXPECTED);
    }

    public void testUnionWithAggregation() throws Exception {
        final String schema = getSchemaWith
          (""
            + " <Role name=\"Foo\"> \n"
            + "     <SchemaGrant access=\"none\"> \n"
            + "     </SchemaGrant> \n"
            + " </Role>\n"
            + " <Role name=\"Power User\"> \n"
            + "     <SchemaGrant access=\"none\"> \n"
            + "         <CubeGrant cube=\"Customers Cube\" access=\"all\"> \n"
            + "             <DimensionGrant dimension=\"Measures\" access=\"all\"> \n"
            + "             </DimensionGrant>\n"
            + "             <HierarchyGrant hierarchy=\"[Customer_DimUsage.Customers Hierarchy]\" topLevel=\"[Customer_DimUsage.Customers Hierarchy].[Name]\" "
            + "                 rollupPolicy=\"partial\" access=\"custom\"> \n"
            + "                 <MemberGrant member=\"[Customer_DimUsage.Customers Hierarchy].[1 rue Alsace-Lorraine].[Roulet]\" access=\"all\"> \n"
            + "                 </MemberGrant> \n"
            + "             </HierarchyGrant> \n"
            + "         </CubeGrant> \n"
            + "     </SchemaGrant> \n"
            + " </Role>\n"
            + " <Role name=\"Power User Union\"> \n"
            + "     <Union> \n"
            + "         <RoleUsage roleName=\"Power User\"/> \n"
            + "         <RoleUsage roleName=\"Foo\"/> \n"
            + "     </Union> \n"
            + " </Role>\n");

        getTestContext()
          .withSchema(schema)
          .withRole("Power User Union")
          .assertQueryReturns(QUERY, EXPECTED);
    }

    public void testWithAggregationUnionRolesWithSameGrants() throws Exception {
        final String schema = getSchemaWith
          (""
          + " <Role name=\"Foo\"> \n"
          + "     <SchemaGrant access=\"none\"> \n"
          + "         <CubeGrant cube=\"Customers Cube\" access=\"all\"> \n"
          + "             <DimensionGrant dimension=\"Measures\" access=\"all\"> \n"
          + "             </DimensionGrant>\n"
          + "             <HierarchyGrant hierarchy=\"[Customer_DimUsage.Customers Hierarchy]\" topLevel=\"[Customer_DimUsage.Customers Hierarchy].[Name]\" "
          + "               rollupPolicy=\"partial\" access=\"custom\"> \n"
          + "                 <MemberGrant member=\"[Customer_DimUsage.Customers Hierarchy].[1 rue Alsace-Lorraine].[Roulet]\" access=\"all\"> \n"
          + "                 </MemberGrant> \n"
          + "             </HierarchyGrant> \n"
          + "         </CubeGrant> \n"
          + "     </SchemaGrant> \n"
          + " </Role>\n"
          + " <Role name=\"Power User\"> \n"
          + "     <SchemaGrant access=\"none\"> \n"
          + "         <CubeGrant cube=\"Customers Cube\" access=\"all\"> \n"
          + "             <DimensionGrant dimension=\"Measures\" access=\"all\"> \n"
          + "             </DimensionGrant>\n"
          + "             <HierarchyGrant hierarchy=\"[Customer_DimUsage.Customers Hierarchy]\" topLevel=\"[Customer_DimUsage.Customers Hierarchy].[Name]\" "
          +                   "rollupPolicy=\"partial\" access=\"custom\"> \n"
          + "                 <MemberGrant member=\"[Customer_DimUsage.Customers Hierarchy].[1 rue Alsace-Lorraine].[Roulet]\" access=\"all\"> \n"
          + "                 </MemberGrant> \n"
          + "             </HierarchyGrant> \n"
          + "         </CubeGrant> \n"
          + "     </SchemaGrant> \n"
          + " </Role>\n"
          + " <Role name=\"Power User Union\"> \n"
          + "     <Union> \n"
          + "         <RoleUsage roleName=\"Power User\"/> \n"
          + "         <RoleUsage roleName=\"Foo\"/> \n"
          + "     </Union> \n"
          + " </Role>\n");

        getTestContext()
          .withSchema(schema)
          .withRole("Power User Union")
          .assertQueryReturns(QUERY, EXPECTED);
    }
}
// End SteelWheelsAggregationTest.java
