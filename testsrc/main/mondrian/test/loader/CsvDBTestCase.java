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
import mondrian.test.FoodMartTestCase;
import java.sql.SQLException;
import java.sql.Connection;
import java.io.File;

/** 
 * This abstract class supports the creation of test that use
 * a CSV database defined in a single file. While the CsvDBLoader
 * supports being defined by a single file, list of files, or
 * directory with optional regular expression for matching files
 * in the directory to be loaded, this is simplest at this point.
 * <p>
 * To use this file one must define both the directory and file
 * abstract methods.
 * 
 * @author <a>Richard M. Emberson</a>
 * @version 
 */
public abstract class CsvDBTestCase extends FoodMartTestCase {

    private CsvDBLoader loader;
    private CsvDBLoader.Table[] tables;

    public CsvDBTestCase() {
        super();
    }
    public CsvDBTestCase(String name) {
        super(name);
    }


    protected void setUp() throws Exception {
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
    }
    protected void tearDown() throws Exception {
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

    protected abstract String getDirectoryName();
    protected abstract String getFileName();
}
