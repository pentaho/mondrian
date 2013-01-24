/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2012-2013 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.test.TestContext;
import mondrian.util.DelegatingInvocationHandler;

import junit.framework.TestCase;

import java.lang.reflect.Proxy;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Unit test for {@link DateTableBuilder}.
 *
 * @author jhyde
 */
public class DateTableBuilderTest extends TestCase {
    public DateTableBuilderTest() {
        super();
    }

    public DateTableBuilderTest(String name) {
        super(name);
    }

    public void testDatePopulater() throws SQLException {
        final StringBuilder buf = new StringBuilder();
        PreparedStatement pstmt = createMockPreparedStatement(buf);
        List<TimeColumnRole.Struct> roles = createStructs(buf);
        DateTableBuilder.populate(
            roles,
            pstmt,
            createDate(2011, 12, 30),
            createDate(2012, 3, 2),
            Locale.US);
        TestContext.assertEqualsVerbose(
            "JULIAN|YYMMDD|YYYYMMDD|DATE|DAY_OF_WEEK|DAY_OF_WEEK_IN_MONTH|DAY_OF_WEEK_NAME|MONTH_NAME|YEAR|DAY_OF_MONTH|WEEK_OF_YEAR|MONTH|QUARTER|\n"
            + "2455926|111230|20111230|2011-12-30|6|5|Friday|December|2011|30|53|12|Q4|\n"
            + "2455927|111231|20111231|2011-12-31|7|5|Saturday|December|2011|31|53|12|Q4|\n"
            + "2455928|120101|20120101|2012-01-01|1|1|Sunday|January|2012|1|1|1|Q1|\n"
            + "2455929|120102|20120102|2012-01-02|2|1|Monday|January|2012|2|1|1|Q1|\n"
            + "2455930|120103|20120103|2012-01-03|3|1|Tuesday|January|2012|3|1|1|Q1|\n"
            + "2455931|120104|20120104|2012-01-04|4|1|Wednesday|January|2012|4|1|1|Q1|\n"
            + "2455932|120105|20120105|2012-01-05|5|1|Thursday|January|2012|5|1|1|Q1|\n"
            + "2455933|120106|20120106|2012-01-06|6|1|Friday|January|2012|6|1|1|Q1|\n"
            + "2455934|120107|20120107|2012-01-07|7|1|Saturday|January|2012|7|1|1|Q1|\n"
            + "2455935|120108|20120108|2012-01-08|1|2|Sunday|January|2012|8|2|1|Q1|\n"
            + "2455936|120109|20120109|2012-01-09|2|2|Monday|January|2012|9|2|1|Q1|\n"
            + "2455937|120110|20120110|2012-01-10|3|2|Tuesday|January|2012|10|2|1|Q1|\n"
            + "2455938|120111|20120111|2012-01-11|4|2|Wednesday|January|2012|11|2|1|Q1|\n"
            + "2455939|120112|20120112|2012-01-12|5|2|Thursday|January|2012|12|2|1|Q1|\n"
            + "2455940|120113|20120113|2012-01-13|6|2|Friday|January|2012|13|2|1|Q1|\n"
            + "2455941|120114|20120114|2012-01-14|7|2|Saturday|January|2012|14|2|1|Q1|\n"
            + "2455942|120115|20120115|2012-01-15|1|3|Sunday|January|2012|15|3|1|Q1|\n"
            + "2455943|120116|20120116|2012-01-16|2|3|Monday|January|2012|16|3|1|Q1|\n"
            + "2455944|120117|20120117|2012-01-17|3|3|Tuesday|January|2012|17|3|1|Q1|\n"
            + "2455945|120118|20120118|2012-01-18|4|3|Wednesday|January|2012|18|3|1|Q1|\n"
            + "2455946|120119|20120119|2012-01-19|5|3|Thursday|January|2012|19|3|1|Q1|\n"
            + "2455947|120120|20120120|2012-01-20|6|3|Friday|January|2012|20|3|1|Q1|\n"
            + "2455948|120121|20120121|2012-01-21|7|3|Saturday|January|2012|21|3|1|Q1|\n"
            + "2455949|120122|20120122|2012-01-22|1|4|Sunday|January|2012|22|4|1|Q1|\n"
            + "2455950|120123|20120123|2012-01-23|2|4|Monday|January|2012|23|4|1|Q1|\n"
            + "2455951|120124|20120124|2012-01-24|3|4|Tuesday|January|2012|24|4|1|Q1|\n"
            + "2455952|120125|20120125|2012-01-25|4|4|Wednesday|January|2012|25|4|1|Q1|\n"
            + "2455953|120126|20120126|2012-01-26|5|4|Thursday|January|2012|26|4|1|Q1|\n"
            + "2455954|120127|20120127|2012-01-27|6|4|Friday|January|2012|27|4|1|Q1|\n"
            + "2455955|120128|20120128|2012-01-28|7|4|Saturday|January|2012|28|4|1|Q1|\n"
            + "2455956|120129|20120129|2012-01-29|1|5|Sunday|January|2012|29|5|1|Q1|\n"
            + "2455957|120130|20120130|2012-01-30|2|5|Monday|January|2012|30|5|1|Q1|\n"
            + "2455958|120131|20120131|2012-01-31|3|5|Tuesday|January|2012|31|5|1|Q1|\n"
            + "2455959|120201|20120201|2012-02-01|4|1|Wednesday|February|2012|1|5|2|Q1|\n"
            + "2455960|120202|20120202|2012-02-02|5|1|Thursday|February|2012|2|5|2|Q1|\n"
            + "2455961|120203|20120203|2012-02-03|6|1|Friday|February|2012|3|5|2|Q1|\n"
            + "2455962|120204|20120204|2012-02-04|7|1|Saturday|February|2012|4|5|2|Q1|\n"
            + "2455963|120205|20120205|2012-02-05|1|1|Sunday|February|2012|5|6|2|Q1|\n"
            + "2455964|120206|20120206|2012-02-06|2|1|Monday|February|2012|6|6|2|Q1|\n"
            + "2455965|120207|20120207|2012-02-07|3|1|Tuesday|February|2012|7|6|2|Q1|\n"
            + "2455966|120208|20120208|2012-02-08|4|2|Wednesday|February|2012|8|6|2|Q1|\n"
            + "2455967|120209|20120209|2012-02-09|5|2|Thursday|February|2012|9|6|2|Q1|\n"
            + "2455968|120210|20120210|2012-02-10|6|2|Friday|February|2012|10|6|2|Q1|\n"
            + "2455969|120211|20120211|2012-02-11|7|2|Saturday|February|2012|11|6|2|Q1|\n"
            + "2455970|120212|20120212|2012-02-12|1|2|Sunday|February|2012|12|7|2|Q1|\n"
            + "2455971|120213|20120213|2012-02-13|2|2|Monday|February|2012|13|7|2|Q1|\n"
            + "2455972|120214|20120214|2012-02-14|3|2|Tuesday|February|2012|14|7|2|Q1|\n"
            + "2455973|120215|20120215|2012-02-15|4|3|Wednesday|February|2012|15|7|2|Q1|\n"
            + "2455974|120216|20120216|2012-02-16|5|3|Thursday|February|2012|16|7|2|Q1|\n"
            + "2455975|120217|20120217|2012-02-17|6|3|Friday|February|2012|17|7|2|Q1|\n"
            + "2455976|120218|20120218|2012-02-18|7|3|Saturday|February|2012|18|7|2|Q1|\n"
            + "2455977|120219|20120219|2012-02-19|1|3|Sunday|February|2012|19|8|2|Q1|\n"
            + "2455978|120220|20120220|2012-02-20|2|3|Monday|February|2012|20|8|2|Q1|\n"
            + "2455979|120221|20120221|2012-02-21|3|3|Tuesday|February|2012|21|8|2|Q1|\n"
            + "2455980|120222|20120222|2012-02-22|4|4|Wednesday|February|2012|22|8|2|Q1|\n"
            + "2455981|120223|20120223|2012-02-23|5|4|Thursday|February|2012|23|8|2|Q1|\n"
            + "2455982|120224|20120224|2012-02-24|6|4|Friday|February|2012|24|8|2|Q1|\n"
            + "2455983|120225|20120225|2012-02-25|7|4|Saturday|February|2012|25|8|2|Q1|\n"
            + "2455984|120226|20120226|2012-02-26|1|4|Sunday|February|2012|26|9|2|Q1|\n"
            + "2455985|120227|20120227|2012-02-27|2|4|Monday|February|2012|27|9|2|Q1|\n"
            + "2455986|120228|20120228|2012-02-28|3|4|Tuesday|February|2012|28|9|2|Q1|\n"
            + "2455987|120229|20120229|2012-02-29|4|5|Wednesday|February|2012|29|9|2|Q1|\n"
            + "2455988|120301|20120301|2012-03-01|5|1|Thursday|March|2012|1|9|3|Q1|\n",
            buf.toString());
    }

    /**
     * Tests populating a julian date with an epoch of 1996. This creates a
     * time_id column compatible with FoodMart.
     *
     * @throws SQLException on error
     */
    public void testDatePopulater1996() throws SQLException {
        final StringBuilder buf = new StringBuilder();
        PreparedStatement pstmt = createMockPreparedStatement(buf);
        List<TimeColumnRole.Struct> roles =
            new ArrayList<TimeColumnRole.Struct>();
        roles.add(new TimeColumnRole.Struct(TimeColumnRole.YYMMDD, null));
        buf.append("yymmdd|");
        Date epoch = createDate(1996, 1, 1);
        roles.add(new TimeColumnRole.Struct(TimeColumnRole.JULIAN, epoch));
        buf.append("time_id|");
        buf.append("\n");
        DateTableBuilder.populate(
            roles,
            pstmt,
            createDate(1996, 12, 30),
            createDate(1997, 1, 2),
            Locale.US);
        TestContext.assertEqualsVerbose(
            "yymmdd|time_id|\n"
            + "961230|364|\n"
            + "961231|365|\n"
            + "970101|366|\n",
            buf.toString());
    }

    private List<TimeColumnRole.Struct> createStructs(StringBuilder buf) {
        List<TimeColumnRole.Struct> roles =
            new ArrayList<TimeColumnRole.Struct>();
        for (TimeColumnRole role : TimeColumnRole.values()) {
            roles.add(new TimeColumnRole.Struct(role, null));
            buf.append(role).append("|");
        }
        buf.append("\n");
        return roles;
    }

    private Date createDate(int year, int month, int day) {
        return new Date(year - 1900, month - 1, day);
    }

    public void testDatePopulaterFrench() throws SQLException {
        final StringBuilder buf = new StringBuilder();
        PreparedStatement pstmt = createMockPreparedStatement(buf);
        DateTableBuilder.populate(
            createStructs(buf),
            pstmt,
            createDate(2011, 12, 30),
            createDate(2012, 1, 3),
            Locale.FRANCE);
        TestContext.assertEqualsVerbose(
            "JULIAN|YYMMDD|YYYYMMDD|DATE|DAY_OF_WEEK|DAY_OF_WEEK_IN_MONTH|DAY_OF_WEEK_NAME|MONTH_NAME|YEAR|DAY_OF_MONTH|WEEK_OF_YEAR|MONTH|QUARTER|\n"
            + "2455926|111230|20111230|2011-12-30|6|5|vendredi|décembre|2011|30|53|12|Q4|\n"
            + "2455927|111231|20111231|2011-12-31|7|5|samedi|décembre|2011|31|53|12|Q4|\n"
            + "2455928|120101|20120101|2012-01-01|1|1|dimanche|janvier|2012|1|1|1|Q1|\n"
            + "2455929|120102|20120102|2012-01-02|2|1|lundi|janvier|2012|2|1|1|Q1|\n",
            buf.toString());
    }

    private PreparedStatement createMockPreparedStatement(StringBuilder buf) {
        return (PreparedStatement) Proxy.newProxyInstance(
            DateTableBuilderTest.class.getClassLoader(),
            new Class[]{PreparedStatement.class},
            new PreparedStatementHandler(buf));
    }

    public static class PreparedStatementHandler
        extends DelegatingInvocationHandler
    {
        private final StringBuilder buf;

        public PreparedStatementHandler(StringBuilder buf) {
            this.buf = buf;
        }

        public void setInt(int ordinal, int value) {
            buf.append(value).append("|");
        }

        public void setLong(int ordinal, long value) {
            buf.append(value).append("|");
        }

        public void setDate(int ordinal, Date value) {
            buf.append(value).append("|");
        }

        public void setString(int ordinal, String value) {
            buf.append(value).append("|");
        }

        public boolean execute() {
            buf.append("\n");
            return true;
        }
    }
}

// End DateTableBuilderTest.java
