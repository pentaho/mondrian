/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2009 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap.aggmatcher;

import mondrian.olap.Result;
import mondrian.test.TestContext;
import mondrian.test.loader.CsvDBTestCase;

/**
 * Checkin 7641 attempted to correct a problem demonstrated by this
 * junit. The original problem involved implicit Time member usage in
 * on axis and the use of the default Time member in the other axis.
 * This junit defines a hierarchy Product with a default member 'Class2',
 * The MDX in one axis explicitly uses the {Product][Class1] member.
 * Depending upon whether the 7641 code is used or not (its use
 * depends upon the existance of a System property) one gets different
 * answers when the mdx is evaluated.
 *
 * @author Richard M. Emberson
 */
public class Checkin_7641 extends CsvDBTestCase {
    private static final String DIRECTORY =
        "target/test-classes/mondrian/rolap/aggmatcher";
    private static final String CHECKIN_7641 = "Checkin_7641.csv";

    public static final String PROP_NAME =  "mondrian.test.checkin.7641";

    //private boolean useImplicitMembers;
    public Checkin_7641() {
        super();
    }
    public Checkin_7641(String name) {
        super(name);
    }
    protected void setUp() throws Exception {
        super.setUp();
    }
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testImplicitMember() throws Exception {
        // explicit use of [Product].[Class1]
        String mdx =
            " select NON EMPTY Crossjoin("
            + " Hierarchize(Union({[Product].[Class1]}, "
            + "[Product].[Class1].Children)), "
            + " {[Measures].[Requested Value], "
            + " [Measures].[Shipped Value]}"
            + ") ON COLUMNS,"
            + " NON EMPTY Hierarchize(Union({[Geography].[All Regions]},"
            + "[Geography].[All Regions].Children)) ON ROWS"
            + " from [ImplicitMember]";

        Result result1 = getCubeTestContext().executeQuery(mdx);
        String resultString1 = TestContext.toString(result1);
        Result result2 = getCubeTestContext().executeQuery(mdx);
        String resultString2 = TestContext.toString(result2);

        assertEquals(resultString1, resultString2);
    }

    protected String getDirectoryName() {
        return DIRECTORY;
    }
    protected String getFileName() {
        return CHECKIN_7641;
    }

    protected String getCubeDescription() {
        // defines [Product].[Class2] as default (implicit) member
        return
            "<Cube name='ImplicitMember'>\n"
            + "<Table name='checkin7641'/>\n"
            + "<Dimension name='Geography' foreignKey='cust_loc_id'>\n"
            + "    <Hierarchy hasAll='true' allMemberName='All Regions' defaultMember='' primaryKey='cust_loc_id'>\n"
            + "    <Table name='geography7641'/>\n"
            + "    <Level column='state_cd' name='State' type='String' uniqueMembers='true'/>\n"
            + "    <Level column='city_nm' name='City' type='String' uniqueMembers='true'/>\n"
            + "    <Level column='zip_cd' name='Zip Code' type='String' uniqueMembers='true'/>\n"
            + "    </Hierarchy>\n"
            + "</Dimension>\n"
            + "<Dimension name='Product' foreignKey='prod_id'>\n"
            + "    <Hierarchy hasAll='false' defaultMember='Class2' primaryKey='prod_id'>\n"
            + "    <Table name='prod7611'/>\n"
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

// End Checkin_7641.java
