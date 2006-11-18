/*
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2006-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import java.util.Arrays;
import java.util.Map;

import mondrian.rolap.sql.SqlQuery;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.olap.MondrianProperties;
import mondrian.olap.MondrianDef;

/**
 * Constraint which optimizes the search for a child by name. This is used
 * whenever the string representation of a member is parsed, e.g.
 * [Customers].[All Customers].[USA].[CA]. Restricts the result to
 * the member we are searching for.
 *
 * @author avix
 * @version $Id$
 */
class ChildByNameConstraint extends DefaultMemberChildrenConstraint {
    String childName;
    Object cacheKey;

    /**
     * Creates a <code>ChildByNameConstraint</code>.
     *
     * @param childName Name of child
     */
    public ChildByNameConstraint(String childName) {
        this.childName = childName;
        this.cacheKey = Arrays.asList(
                new Object[] {
                    super.getCacheKey(),
                    ChildByNameConstraint.class, childName});
    }

    public void addLevelConstraint(
        SqlQuery query, AggStar aggStar, RolapLevel level, Map levelToColumnMap)
    {
        super.addLevelConstraint(query, aggStar, level, levelToColumnMap);
        MondrianDef.Expression exp = level.getNameExp();
        SqlQuery.Datatype datatype;
        if (exp == null) {
            exp = level.getKeyExp();
            datatype = level.getDatatype();
        } else {
            // The schema doesn't specify the datatype of the name column, but
            // we presume that it is a string.
            datatype = SqlQuery.Datatype.String;
        }
        String column = exp.getExpression(query);
        String value = childName;
        if (datatype == SqlQuery.Datatype.String) {
            // some dbs (like DB2) compare case sensitive
            if (!MondrianProperties.instance().CaseSensitive.get()) {
                column = query.getDialect().toUpper(column);
                value = value.toUpperCase();
            }
        }
        if (RolapUtil.mdxNullLiteral.equalsIgnoreCase(value)) {
            query.addWhere(
                column,
                " is ",
                RolapUtil.sqlNullLiteral);
        } else {
            if (datatype.isNumeric()) {
                // make sure it can be parsed
                Double.valueOf(value);
            }
            final StringBuffer buf = new StringBuffer();
            query.getDialect().quote(buf, value, datatype);
            query.addWhere(
                column,
                " = ",
                buf.toString());
        }
    }

    public String toString() {
        return "ChildByNameConstraint(" + childName + ")";
    }

    public Object getCacheKey() {
        return cacheKey;
    }

}

// End ChildByNameConstraint.java
