/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2012 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap.aggmatcher;

import mondrian.olap.MondrianProperties;
import mondrian.olap.Result;
import mondrian.test.TestContext;
import mondrian.test.loader.CsvDBTestCase;

/**
 * Checkin 7634 attempted to correct a problem demonstrated by this
 * junit. The CrossJoinFunDef class has an optimization that kicks in
 * when the combined lists sizes are greater than 1000. I create a
 * property here which, if set, can be used to change that size from
 * 1000 to, in this case, 2. Also, there is a property that disables the
 * use of the optimization altogether and another that permits the
 * use of the old optimization, currently the nonEmptyListOld method in
 * the CrossJoinFunDef class, and the new, checkin 7634, version of the
 * method called nonEmptyList.
 *
 * <p>The old optimization only looked at the default measure while the
 * new version looks at all measures appearing in the query.
 * The example Cube and data for the junit is such that there is no
 * data for the default measure. Thus the old optimization fails
 * to produce the correct result.
 *
 * @author Richard M. Emberson
  */
public class Checkin_7634 extends CsvDBTestCase {
    private static final String DIRECTORY =
        "target/test-classes/mondrian/rolap/aggmatcher";

    private static final String CHECKIN_7634 = "Checkin_7634.csv";

    private int crossJoinSize;

    public Checkin_7634() {
        super();
    }

    public Checkin_7634(String name) {
        super(name);
    }

    public void testCrossJoin() throws Exception {
        // explicit use of [Product].[Class1]
        String mdx =
        "select {[Measures].[Requested Value]} ON COLUMNS,"+
        " NON EMPTY Crossjoin("+
        " {[Geography].[All Regions].Children},"+
        " {[Product].[All Products].Children}"+
        ") ON ROWS"+
        " from [Checkin_7634]";


        // Execute query but do not used the CrossJoin nonEmptyList optimization
        propSaver.set(
            propSaver.props.CrossJoinOptimizerSize,
            Integer.MAX_VALUE);
        Result result1 = getCubeTestContext().executeQuery(mdx);
        String resultString1 = TestContext.toString(result1);

        // Execute query using the new version of the CrossJoin
        // nonEmptyList optimization
        propSaver.set(
            propSaver.props.CrossJoinOptimizerSize,
            Integer.MAX_VALUE);
        Result result2 = getCubeTestContext().executeQuery(mdx);
        String resultString2 = TestContext.toString(result2);

        // This succeeds.
        assertEquals(resultString1, resultString2);
    }

    protected String getDirectoryName() {
        return DIRECTORY;
    }
    protected String getFileName() {
        return CHECKIN_7634;
    }

    protected String getCubeDescription() {
        // defines [Product].[Class2] as default (implicit) member
        return
            "<Cube name='Checkin_7634'>\n"
            + "<Table name='table7634'/>\n"
            + "<Dimension name='Geography' foreignKey='cust_loc_id'>\n"
            + "    <Hierarchy hasAll='true' allMemberName='All Regions' defaultMember='' primaryKey='cust_loc_id'>\n"
            + "    <Table name='geography7631'/>\n"
            + "    <Level column='state_cd' name='State' type='String' uniqueMembers='true'/>\n"
            + "    <Level column='city_nm' name='City' type='String' uniqueMembers='true'/>\n"
            + "    <Level column='zip_cd' name='Zip Code' type='String' uniqueMembers='true'/>\n"
            + "    </Hierarchy>\n"
            + "</Dimension>\n"
            + "<Dimension name='Product' foreignKey='prod_id'>\n"
            + "    <Hierarchy hasAll='true' allMemberName='All Products' defaultMember='' primaryKey='prod_id'>\n"
            + "    <Table name='prod7631'/>\n"
            + "    <Level column='class' name='Class' type='String' uniqueMembers='true'/>\n"
            + "    <Level column='brand' name='Brand' type='String' uniqueMembers='true'/>\n"
            + "    <Level column='item' name='Item' type='String' uniqueMembers='true'/>\n"
            + "    </Hierarchy>\n"
            + "</Dimension>\n"
            + "<Measure name='First Measure' \n"
            + "    column='first' aggregator='sum'\n"
            + "   formatString='#,###'/>\n"
            + "<Measure name='Requested Value' \n"
            + "    column='request_value' aggregator='sum'\n"
            + "   formatString='#,###'/>\n"
            + "<Measure name='Shipped Value' \n"
            + "    column='shipped_value' aggregator='sum'\n"
            + "   formatString='#,###'/>\n"
            + "</Cube>";
    }
}

// End Checkin_7634.java
