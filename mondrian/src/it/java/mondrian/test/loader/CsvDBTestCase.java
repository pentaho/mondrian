/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/
package mondrian.test.loader;

import mondrian.olap.Schema;
import mondrian.rolap.BatchTestCase;
import mondrian.spi.Dialect;
import mondrian.test.FoodMartTestCase;
import mondrian.test.TestContext;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;

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
 */
public abstract class CsvDBTestCase extends BatchTestCase {

    private CsvDBLoader loader;
    private CsvDBLoader.Table[] tables;
    private TestContext testContext;

    public CsvDBTestCase() {
        super();
    }

    public CsvDBTestCase(String name) {
        super(name);
    }

    protected final boolean isApplicable() {
        final Dialect dialect = TestContext.instance().getDialect();
        return dialect.allowsDdl()
            && dialect.getDatabaseProduct()
            != Dialect.DatabaseProduct.INFOBRIGHT;
    }

    protected void setUp() throws Exception {
        // If this database does not allow DDL, the test won't run. Don't bother
        // setting up.
        if (!isApplicable()) {
            return;
        }

        super.setUp();

        Connection connection = getSqlConnection();

        File inputFile = new File(getFilePath());

        this.loader = new CsvDBLoader();
        this.loader.setConnection(connection);
        this.loader.initialize();
        this.loader.setInputFile(inputFile);
        this.tables = this.loader.getTables();
        this.loader.generateStatements(this.tables);

        // create database tables
        this.loader.executeStatements(this.tables);

        this.testContext = createTestContext();
    }

    protected TestContext createTestContext() {
        String parameterDefs = getParameterDescription();
        String cubeDefs = getCubeDescription();
        String virtualCubeDefs = getVirtualCubeDescription();
        String namedSetDefs = getNamedSetDescription();
        String udfDefs = getUdfDescription();
        String roleDefs = getRoleDescription();
        return TestContext.instance().create(
            parameterDefs,
            cubeDefs,
            virtualCubeDefs,
            namedSetDefs,
            udfDefs,
            roleDefs);
    }

    @Override
    public TestContext getTestContext() {
        // Return default test context if we haven't yet completed setUp.
        return testContext == null
            ? TestContext.instance()
            : testContext;
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
        try {
            this.loader.close();
        } catch (Exception ex) {
            // ignore
        }

        tables = null;
        testContext = null; // allow gc
        loader = null;

        super.tearDown();
    }

    protected Connection getSqlConnection() throws SQLException {
        return getConnection().getDataSource().getConnection();
    }

    protected Schema getSchema() {
        return getConnection().getSchema();
    }

    protected abstract String getFileName();

    protected String getFilePath() {
        return getClass().getResource(getFileName()).getPath();
    }


    protected String getParameterDescription() {
        return null;
    }

    protected String getCubeDescription() {
        throw new UnsupportedOperationException();
    }

    protected String getVirtualCubeDescription() {
        return null;
    }

    protected String getNamedSetDescription() {
        return null;
    }

    protected String getUdfDescription() {
        return null;
    }

    protected String getRoleDescription() {
        return null;
    }
}

// End CsvDBTestCase.java
