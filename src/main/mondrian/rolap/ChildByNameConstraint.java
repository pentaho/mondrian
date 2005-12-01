/*
//This software is subject to the terms of the Common Public License
//Agreement, available at the following URL:
//http://www.opensource.org/licenses/cpl.html.
//Copyright (C) 2004-2005 TONBELLER AG
//All Rights Reserved.
//You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import java.util.Arrays;

import mondrian.rolap.sql.SqlQuery;

/**
 * optimize the search for a child by name. This is used whenever the
 * string representation of a member is parsed, e.g.
 * [Customers].[All Customers].[USA].[CA]. Restricts the result to
 * the member we are searching for.
 */
class ChildByNameConstraint extends DefaultMemberChildrenConstraint {
    String childName;
    Object cacheKey;

    public ChildByNameConstraint(String childName) {
        this.childName = childName;
        this.cacheKey = Arrays.asList(new Object[] { super.getCacheKey(),
                ChildByNameConstraint.class, childName});
    }

    public void addLevelConstraint(SqlQuery query, RolapLevel level) {
        super.addLevelConstraint(query, level);
        String column = level.getKeyExp().getExpression(query);
        String value = childName;
        if (!level.isNumeric()) {
            // some dbs (like DB2) compare case sensitive
            column = query.getDialect().toUpper(column);
            value = value.toUpperCase();
        }
        value = query.quote(level.isNumeric(), value);
        query.addWhere(column, "=", value);
    }

    public String toString() {
        return "ChildByNameConstraint(" + childName + ")";
    }

    public Object getCacheKey() {
        return cacheKey;
    }

}
