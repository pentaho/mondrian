/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2006 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.test.loader;

import mondrian.olap.Schema;
import mondrian.rolap.RolapConnection;
import mondrian.rolap.sql.SqlQuery;
import mondrian.test.FoodMartTestCase;
import mondrian.test.TestContext;
import java.sql.SQLException;
import java.sql.Connection;
import java.io.File;

/** 
 * Base class for tests that use
 * a CSV database defined in a single file. While the CsvDBLoader
 * supports being defined by a single file, list of files, or
 * directory with optional regular expression for matching files
 * in the directory to be loaded, this is simplest at this point.
 *
 * <p>
 * To use this file one must define both the directory and file
 * abstract methods.
 * 
 * @author Richard M. Emberson
 * @version $Id$
 */
public abstract class CsvDBTestCase extends FoodMartTestCase {

    private CsvDBLoader loader;
    private CsvDBLoader.Table[] tables;
    private TestContext testContext;
    protected final boolean applicable;

    public CsvDBTestCase() {
        super();
        applicable = getTestContext().getDialect().allowsDdl();
    }

    public CsvDBTestCase(String name) {
        super(name);
        applicable = getTestContext().getDialect().allowsDdl();
    }

    protected final boolean isApplicable() {
        return applicable;
    }

    protected void setUp() throws Exception {
        // If this database does not allow DDL, the test won't run. Don't bother
        // setting up.
        if (!isApplicable()) {
            return;
        }

        super.setUp();

        Connection connection = getSqlConnection();
        String dirName = getDirectoryName();

        String fileName = getFileName();
        File inputFile = new File(dirName, fileName);

        this.loader = new CsvDBLoader();
        this.loader.setConnection(connection);
        this.loader.initialize();
        this.loader.setInputFile(inputFile);
        this.tables = this.loader.getTables();
        this.loader.generateStatements(this.tables);

        // create database tables
        this.loader.executeStatements(this.tables);

        String cubeDescription = getCubeDescription();
        this.testContext = TestContext.create(
            null, cubeDescription, null, null, null);
    }

    protected void tearDown() throws Exception {
        // If this database does not allow DDL, we didn't run setUp; so, nothing
        // to tear down.
        if (!isApplicable()) {
            return;
        }

        try {
            // drop database tables
            this.loader.dropTables(this.tables);
        } catch (Exception ex) {
            // ignore
        }

        super.tearDown();
    }

    protected Connection getSqlConnection() throws SQLException {
        return ((RolapConnection) getConnection()).getDataSource().getConnection();
    }

    protected Schema getSchema() {
        return getConnection().getSchema();
    }

    protected TestContext getCubeTestContext() {
        return testContext;
    }

    protected abstract String getDirectoryName();
    protected abstract String getFileName();
    protected abstract String getCubeDescription();
}

// End CsvDBTestCase.java
