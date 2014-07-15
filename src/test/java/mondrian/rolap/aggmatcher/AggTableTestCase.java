/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2013 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap.aggmatcher;

import mondrian.test.loader.CsvDBTestCase;

/**
 * This abstract class can be used as the basis for writing aggregate table
 * test in the "testsrc/main/mondrian/rolap/aggmatcher" directory. Taken care
 * of is the setting of the Caching and Aggregate Read/Use properties and
 * the reloading of the aggregate tables after the CSV tables are loaded.
 * The particular cube definition and CSV file to use are abstract methods.
 *
 * @author Richard M. Emberson
 */
public abstract class AggTableTestCase extends CsvDBTestCase {

    private static final String DIRECTORY =
        "target/test-classes/mondrian/rolap/aggmatcher";

    public AggTableTestCase() {
        super();
    }

    public AggTableTestCase(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        // Schema needs to be flushed before DBLoader is created is super.setUp,
        // otherwise AggTableManager can end up loading an old JdbcSchema
        getConnection().getCacheControl(null).flushSchemaCache();

        super.setUp();

        // turn off caching
        propSaver.set(propSaver.props.DisableCaching, true);
    }

    protected String getDirectoryName() {
        return DIRECTORY;
    }
}

// End AggTableTestCase.java
