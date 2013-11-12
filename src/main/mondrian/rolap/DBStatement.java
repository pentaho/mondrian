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

// FIXME MONGO DOCUMENT THIS AND ADD MISSING METHODS.
public interface DBStatement {
    // FIXME MONGO clean this up.
    public Map<Object, Accessor> getAccessors() throws SQLException;
}
// End DBStatement.java