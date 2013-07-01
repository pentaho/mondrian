/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2012-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.Util;
import mondrian.spi.Dialect;
import mondrian.util.Format;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * Enumeration of the types of column that can be in a date dimension table.
 *
 * @author jhyde
*/
enum TimeColumnRole {
    JULIAN("time_id", Dialect.Datatype.Integer), // e.g. 2454115
    YYMMDD("yymmdd", Dialect.Datatype.Integer), // e.g. 121231
    YYYYMMDD("yyyymmdd", Dialect.Datatype.Integer), // e.g. 20121231
    DATE("the_date", Dialect.Datatype.Date), // e.g. '2012-12-31'
    DAY_OF_WEEK("day_of_week", Dialect.Datatype.Integer), // e.g. 0 (= Sunday)
    DAY_OF_WEEK_IN_MONTH("day_of_week_in_month", Dialect.Datatype.Integer),
    DAY_OF_WEEK_NAME("the_day", Dialect.Datatype.String), // e.g. 'Friday'
    MONTH_NAME("the_month", Dialect.Datatype.String), // e.g. 'December'
    YEAR("the_year", Dialect.Datatype.Integer), // e.g. 2012
    DAY_OF_MONTH("day_of_month", Dialect.Datatype.Integer), // e.g. 31
    WEEK_OF_YEAR("week_of_year", Dialect.Datatype.Integer), // e.g. 53
    MONTH("month_of_year", Dialect.Datatype.Integer), // e.g. 12
    QUARTER("quarter", Dialect.Datatype.String); // e.g. 'Q4'

    final String columnName;
    final Dialect.Datatype defaultDatatype;

    public static final Map<String, TimeColumnRole> mapNameToRole;

    private TimeColumnRole(String columnName, Dialect.Datatype datatype) {
        this.columnName = columnName;
        this.defaultDatatype = datatype;
    }

    private static String[] quarters = {
        "Q1", "Q1", "Q1",
        "Q2", "Q2", "Q2",
        "Q3", "Q3", "Q3",
        "Q4", "Q4", "Q4",
    };

    static {
        Map<String, TimeColumnRole> map =
            new HashMap<String, TimeColumnRole>();
        for (TimeColumnRole value : values()) {
            TimeColumnRole put = map.put(value.columnName.toUpperCase(), value);
            assert put == null : "duplicate column";
        }
        mapNameToRole = Collections.unmodifiableMap(map);
    }

    public static class Struct {
        public final TimeColumnRole role;
        public final Date epoch;

        public Struct(TimeColumnRole role, Date epoch) {
            this.role = role;
            this.epoch = epoch;
            assert role != null;
        }

        /**
         * Creates state, if any is needed, that can be used for multiple calls
         * to {@link #bind}.
         *
         * @param locale Locale in which to generate strings
         * @return Any needed state, or null
         */
        Object initialize(Locale locale) {
            switch (role) {
            case DAY_OF_WEEK_NAME:
                return new Format("dddd", locale);
            case MONTH_NAME:
                return new Format("mmmm", locale);
            case JULIAN:
                if (epoch != null) {
                    return Util.julian(
                        epoch.getYear() + 1900,
                        epoch.getMonth() + 1,
                        epoch.getDate());
                } else {
                    return 0L;
                }
            default:
                return null;
            }
        }

        void bind(
            Object[] states,
            int ordinal,
            Calendar calendar,
            PreparedStatement pstmt) throws SQLException
        {
            switch (role) {
            case JULIAN:
                long julian =
                    Util.julian(
                        calendar.get(Calendar.YEAR),
                        calendar.get(Calendar.MONTH) + 1,
                        calendar.get(Calendar.DAY_OF_MONTH))
                    - ((Long) states[ordinal]);
                pstmt.setLong(ordinal, julian);
                return;
            case YYMMDD:
                pstmt.setLong(
                    ordinal,
                    (calendar.get(Calendar.YEAR) % 100) * 10000
                    + (calendar.get(Calendar.MONTH) + 1) * 100
                    + calendar.get(Calendar.DAY_OF_MONTH));
                return;
            case YYYYMMDD:
                pstmt.setLong(
                    ordinal,
                    calendar.get(Calendar.YEAR) * 10000
                    + (calendar.get(Calendar.MONTH) + 1) * 100
                    + calendar.get(Calendar.DAY_OF_MONTH));
                return;
            case DATE:
                pstmt.setDate(
                    ordinal,
                    new java.sql.Date(calendar.getTimeInMillis()));
                return;
            case DAY_OF_MONTH:
                pstmt.setInt(
                    ordinal,
                    calendar.get(Calendar.DAY_OF_MONTH));
                return;
            case MONTH_NAME:
            case DAY_OF_WEEK_NAME:
                pstmt.setString(
                    ordinal, ((Format) states[ordinal]).format(calendar));
                return;
            case DAY_OF_WEEK:
                pstmt.setInt(ordinal, calendar.get(Calendar.DAY_OF_WEEK));
                return;
            case DAY_OF_WEEK_IN_MONTH:
                pstmt.setInt(
                    ordinal, calendar.get(Calendar.DAY_OF_WEEK_IN_MONTH));
                return;
            case WEEK_OF_YEAR:
                pstmt.setInt(ordinal, calendar.get(Calendar.WEEK_OF_YEAR));
                return;
            case MONTH:
                pstmt.setInt(ordinal, calendar.get(Calendar.MONTH) + 1);
                return;
            case QUARTER:
                pstmt.setString(
                    ordinal, quarters[calendar.get(Calendar.MONTH)]);
                return;
            case YEAR:
                pstmt.setInt(
                    ordinal, calendar.get(Calendar.YEAR));
                return;
            default:
                throw Util.unexpected(role);
            }
        }
    }
}

// End TimeColumnRole.java
