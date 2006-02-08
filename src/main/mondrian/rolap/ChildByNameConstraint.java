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

import mondrian.rolap.sql.SqlQuery;
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

    public ChildByNameConstraint(String childName) {
        this.childName = childName;
        this.cacheKey = Arrays.asList(
                new Object[] {
                    super.getCacheKey(),
                    ChildByNameConstraint.class, childName});
    }

    public void addLevelConstraint(SqlQuery query, RolapLevel level) {
        super.addLevelConstraint(query, level);
        MondrianDef.Expression exp = level.getNameExp();
        boolean numeric;
        if (exp == null) {
            exp = level.getKeyExp();
            numeric = level.isNumeric();
        } else {
            // The schema doesn't specify whether the name column is numeric
            // but we presume that it is not.
            numeric = false;
        }
        String column = exp.getExpression(query);
        String value = childName;
        if (!numeric) {
            // some dbs (like DB2) compare case sensitive
            column = query.getDialect().toUpper(column);
            value = value.toUpperCase();
        }
        value = query.quote(numeric, value);
        query.addWhere(column, RolapUtil.sqlNullLiteral.equals(value) ? " is " : " = ", value);
    }

    public String toString() {
        return "ChildByNameConstraint(" + childName + ")";
    }

    public Object getCacheKey() {
        return cacheKey;
    }

}

// End ChildByNameConstraint.java
