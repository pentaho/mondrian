/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap.sql;

import junit.framework.TestCase;
import mondrian.test.FoodMartTestCase;
import mondrian.olap.MondrianProperties;
import mondrian.olap.MondrianDef;
import mondrian.olap.Util;

import java.util.ArrayList;

/**
 * <p>Test for <code>SqlQuery</code></p>
 *
 * @author Thiyagu
 * @version $Id$
 * @since 06-Jun-2007
 */
public class SqlQueryTest extends FoodMartTestCase {


    public void testToStringForSingleGroupingSetSql() {
        if (isGroupingSetsSupported()) {
            MondrianProperties.instance().GenerateFormattedSql.set(true);
            SqlQuery sqlQuery = new SqlQuery(getTestContext().getDialect());
            sqlQuery.addSelect("c1");
            sqlQuery.addSelect("c2");
            sqlQuery.addGroupingFunction("gf0");
            sqlQuery.addFromTable("s", "t1", "t1alias", null, true);
            sqlQuery.addWhere("a=b");
            ArrayList<String> groupingsetsList = new ArrayList<String>();
            groupingsetsList.add("gs1");
            groupingsetsList.add("gs2");
            groupingsetsList.add("gs3");
            sqlQuery.addGroupingSet(groupingsetsList);
            MondrianProperties.instance().GenerateFormattedSql.set(false);
            assertEquals(
                "select c1 as \"c0\", c2 as \"c1\", grouping(gf0) as \"g0\" " +
                    "from \"s\".\"t1\" \"t1alias\" where a=b group by grouping sets ((gs1,gs2,gs3))",
                sqlQuery.toString());
            MondrianProperties.instance().GenerateFormattedSql.set(true);
            String expectedString = "select " + Util.nl +
                "    c1 as \"c0\", " + Util.nl +
                "    c2 as \"c1\"" + Util.nl +
                "    , grouping(gf0) as \"g0\"" + Util.nl +
                "from " + Util.nl +
                "    \"s\".\"t1\" \"t1alias\"" + Util.nl +
                "where " + Util.nl +
                "    a=b" + Util.nl +
                " group by grouping sets ((" + Util.nl +
                "    gs1," + Util.nl +
                "    gs2," + Util.nl +
                "    gs3" + Util.nl +
                "))";
            assertEquals(expectedString, sqlQuery.toString());
        }
    }

    public void testToStringForGroupingSetSqlWithEmptyGroup() {
        if (isGroupingSetsSupported()) {
            MondrianProperties.instance().GenerateFormattedSql.set(true);
            SqlQuery sqlQuery = new SqlQuery(getTestContext().getDialect());
            sqlQuery.addSelect("c1");
            sqlQuery.addSelect("c2");
            sqlQuery.addFromTable("s", "t1", "t1alias", null, true);
            sqlQuery.addWhere("a=b");
            sqlQuery.addGroupingFunction("g1");
            sqlQuery.addGroupingFunction("g2");
            ArrayList<String> groupingsetsList = new ArrayList<String>();
            groupingsetsList.add("gs1");
            groupingsetsList.add("gs2");
            groupingsetsList.add("gs3");
            sqlQuery.addGroupingSet(new ArrayList<String>());
            sqlQuery.addGroupingSet(groupingsetsList);
            MondrianProperties.instance().GenerateFormattedSql.set(false);
            assertEquals(
                "select c1 as \"c0\", c2 as \"c1\", grouping(g1) as \"g0\", " +
                    "grouping(g2) as \"g1\" from \"s\".\"t1\" \"t1alias\" where a=b " +
                    "group by grouping sets ((),(gs1,gs2,gs3))",
                sqlQuery.toString());
            MondrianProperties.instance().GenerateFormattedSql.set(true);
            String expectedString = "select " + Util.nl +
                "    c1 as \"c0\", " + Util.nl +
                "    c2 as \"c1\"" + Util.nl +
                "    , grouping(g1) as \"g0\"" + Util.nl +
                "    , grouping(g2) as \"g1\"" + Util.nl +
                "from " + Util.nl +
                "    \"s\".\"t1\" \"t1alias\"" + Util.nl +
                "where " + Util.nl +
                "    a=b" + Util.nl +
                " group by grouping sets ((),(" + Util.nl +
                "    gs1," + Util.nl +
                "    gs2," + Util.nl +
                "    gs3" + Util.nl +
                "))";
            assertEquals(expectedString, sqlQuery.toString());
        }
    }

    public void testToStringForMultipleGroupingSetsSql() {
        if (isGroupingSetsSupported()) {
            MondrianProperties.instance().GenerateFormattedSql.set(true);
            SqlQuery sqlQuery = new SqlQuery(getTestContext().getDialect());
            sqlQuery.addSelect("c0");
            sqlQuery.addSelect("c1");
            sqlQuery.addSelect("c2");
            sqlQuery.addSelect("m1", "m1");
            sqlQuery.addFromTable("s", "t1", "t1alias", null, true);
            sqlQuery.addWhere("a=b");
            sqlQuery.addGroupingFunction("c0");
            sqlQuery.addGroupingFunction("c1");
            sqlQuery.addGroupingFunction("c2");
            ArrayList<String> groupingSetlist1 = new ArrayList<String>();
            groupingSetlist1.add("c0");
            groupingSetlist1.add("c1");
            groupingSetlist1.add("c2");
            sqlQuery.addGroupingSet(groupingSetlist1);
            ArrayList<String> groupingsetsList2 = new ArrayList<String>();
            groupingsetsList2.add("c1");
            groupingsetsList2.add("c2");
            sqlQuery.addGroupingSet(groupingsetsList2);
            MondrianProperties.instance().GenerateFormattedSql.set(false);
            assertEquals(
                "select c0 as \"c0\", c1 as \"c1\", c2 as \"c2\", m1 as \"m1\", " +
                    "grouping(c0) as \"g0\", grouping(c1) as \"g1\", grouping(c2) as \"g2\" " +
                    "from \"s\".\"t1\" \"t1alias\" where a=b " +
                    "group by grouping sets ((c0,c1,c2),(c1,c2))",
                sqlQuery.toString());
            MondrianProperties.instance().GenerateFormattedSql.set(true);
            String expectedString = "select " + Util.nl +
                "    c0 as \"c0\", " + Util.nl +
                "    c1 as \"c1\", " + Util.nl +
                "    c2 as \"c2\", " + Util.nl +
                "    m1 as \"m1\"" + Util.nl +
                "    , grouping(c0) as \"g0\"" + Util.nl +
                "    , grouping(c1) as \"g1\"" + Util.nl +
                "    , grouping(c2) as \"g2\"" + Util.nl +
                "from " + Util.nl +
                "    \"s\".\"t1\" \"t1alias\"" + Util.nl +
                "where " + Util.nl +
                "    a=b" + Util.nl +
                " group by grouping sets ((" + Util.nl +
                "    c0," + Util.nl +
                "    c1," + Util.nl +
                "    c2" + Util.nl +
                "),(" + Util.nl +
                "    c1," + Util.nl +
                "    c2" + Util.nl +
                "))";
            assertEquals(expectedString, sqlQuery.toString());
        }
    }
}
