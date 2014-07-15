/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2013-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap.sql;

import mondrian.olap.Util;

/**
 * Enumeration that describes which parts of a SQL query a column should be
 * added to.
*/
public enum Clause {
    SELECT, SELECT_ORDER, SELECT_GROUP, SELECT_GROUP_ORDER, FROM;

    public Clause maybeOrder(boolean needsOrderBy) {
        assert this != FROM;
        return values()[Util.bit(ordinal(), 0, needsOrderBy)];
    }

    public Clause maybeGroup(boolean needsGroupBy) {
        assert this != FROM;
        return values()[Util.bit(ordinal(), 1, needsGroupBy)];
    }
}

// End Clause.java
