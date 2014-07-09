/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2013-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.test.data;

import java.io.*;

/**
 * Loads the FoodMart queries data set.
 *
 * <p>Used in maven resource {groupId=pentaho,
 * artifactId=mondrian-data-foodmart-json}.</p>
 */
@SuppressWarnings("UnusedDeclaration")
public class FoodMartJson {
    public FoodMartJson() {
    }

    /** Returns the contents of a table as a stream. For example,
     * getTable("time_by_day") returns the contents of "time_by_day.json" in the
     * current jar file. */
    public InputStream getTable(String tableName) throws IOException {
        return FoodMartJson.class.getClassLoader()
            .getResourceAsStream(tableName + ".json");
    }
}

// End FoodMartJson.java
