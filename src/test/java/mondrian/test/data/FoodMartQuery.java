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
 * artifactId=mondrian-data-foodmart-queries}.</p>
 */
@SuppressWarnings("UnusedDeclaration")
public class FoodMartQuery {
    public FoodMartQuery() {
    }

    /** Returns the contents of "queries.json" in the current jar file as a
     * stream. */
    public InputStream getQueries() throws IOException {
        return FoodMartQuery.class.getClassLoader()
            .getResourceAsStream("queries.json");
    }
}

// End FoodMartQuery.java
