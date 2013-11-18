/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2013-2013 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.rolap.SqlStatement.Accessor;

import java.sql.SQLException;
import java.util.*;

/**
 * Interface for retrieving a Map of Accessors which access database results
 */
public interface DBStatement {
    /**
     * @return map of Accessors.  the keys are Integers in the default
     * implementation; other implementations might use Strings
     */
    public Map<Object, Accessor> getAccessors() throws SQLException;
}
// End DBStatement.java