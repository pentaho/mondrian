/*
// $Id$
// (C) Copyright 2002-2005 Kana Software, Inc.
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 29 March, 2002
*/
package mondrian.test;

import mondrian.olap.*;
import mondrian.rolap.RolapConnectionProperties;

import java.io.File;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * <code>TestContext</code> is a singleton class which contains the information
 * necessary to run mondrian tests (otherwise we'd have to pass this
 * information into the constructor of TestCases).
 *
 * @author jhyde
 * @since 29 March, 2002
 * @version $Id$
 **/
public class TestContext {
    private static TestContext instance; // the singleton
    private PrintWriter pw;
    /** Connect string for the FoodMart database. Set by the constructor,
     * but the connection is not created until the first call to
     * {@link #getFoodMartConnection}. **/
    private String foodMartConnectString;
    /** Connection to the FoodMart database. Set on the first call to
     * {@link #getFoodMartConnection}. **/
    private Connection foodMartConnection;

    /**
     * Retrieves the singleton (instantiating if necessary).
     */
    public static TestContext instance() {
        if (instance == null) {
            synchronized (TestContext.class) {
                if (instance == null) {
                    instance = new TestContext();
                }
            }
        }
        return instance;
    }

    /** Creates a TestContext. Called only from {@link #instance()}. **/
    private TestContext() {
        this.pw = new PrintWriter(System.out, true);
        foodMartConnectString = getConnectString();
    }

    /**
     * Constructs a connect string by which the unit tests can talk to the
     * FoodMart database.
     *
     * The algorithm is as follows:<ul>
     * <li>Starts with {@link MondrianProperties#getTestConnectString}, if it is
     *     set.</li>
     * <li>If {@link MondrianProperties#getFoodmartJdbcURL} is set, this
     *     overrides the <code>Jdbc</code> property.</li>
     * <li>If the <code>catalog</code> URL is unset or invalid, it assumes that
     *     we are at the root of the source tree, and references
     *     <code>demo/FoodMart.xml</code></li>.
     * </ul>
     */
    private static String getConnectString() {
        String connectString = MondrianProperties.instance().getTestConnectString();
        final Util.PropertyList connectProperties;
        if (connectString == null || connectString.equals("")) {
            connectProperties = new Util.PropertyList();
            connectProperties.put("Provider","mondrian");
        } else {
             connectProperties = Util.parseConnectString(connectString);
        }
        String jdbcURL = MondrianProperties.instance().getFoodmartJdbcURL();
        if (jdbcURL != null) {
            connectProperties.put("Jdbc", jdbcURL);
        }
        // Find the catalog. Use the URL specified in the connect string, if
        // it is specified and is valid. Otherwise, reference FoodMart.xml
        // assuming we are at the root of the source tree.
        URL catalogURL = null;
        String catalog = connectProperties.get("catalog");
        if (catalog != null) {
            try {
                catalogURL = new URL(catalog);
            } catch (MalformedURLException e) {
                // ignore
            }
        }
        if (catalogURL == null) {
            // Works if we are running in root directory of source tree
            File file = new File("demo/FoodMart.xml");
            if (!file.exists()) {
                // Works if we are running in bin directory of runtime env
                file = new File("../demo/FoodMart.xml");
            }
            catalogURL = convertPathToURL(file);
        }
        connectProperties.put("catalog", catalogURL.toString());
        return connectProperties.toString();
    }

    /**
     * Creates a file-protocol URL for the given filename.
     **/
    public static URL convertPathToURL(File file)
    {
        try {
            String path = file.getAbsolutePath();
            // This is a bunch of weird code that is required to
            // make a valid URL on the Windows platform, due
            // to inconsistencies in what getAbsolutePath returns.
            String fs = System.getProperty("file.separator");
            if (fs.length() == 1)
            {
                char sep = fs.charAt(0);
                if (sep != '/')
                    path = path.replace(sep, '/');
                if (path.charAt(0) != '/')
                    path = '/' + path;
            }
            path = "file://" + path;
            return new URL(path);
        } catch (MalformedURLException e) {
            throw new java.lang.Error(e.getMessage());
        }
    }

    /** Returns a connection to the FoodMart database. **/
    public synchronized Connection getFoodMartConnection(boolean fresh) {
        if (fresh) {
            return DriverManager.getConnection(
                    foodMartConnectString, null, fresh);
        } else if (foodMartConnection == null) {
            foodMartConnection = DriverManager.getConnection(
                    foodMartConnectString, null, fresh);
        }
        return foodMartConnection;
    }

    /** Returns a connection to the FoodMart database
     * with a dynamic schema processor.
     **/
    public synchronized Connection getFoodMartConnection(String dynProc) {
        Util.PropertyList properties = Util.parseConnectString(foodMartConnectString);
        properties.put(RolapConnectionProperties.DynamicSchemaProcessor, dynProc);
        Connection newConnection = mondrian.olap.DriverManager.getConnection(properties, null, null, false);
        return newConnection;
    }

    /** Executes a query against the FoodMart database. **/
    public Result executeFoodMart(String queryString) {
        Connection connection = getFoodMartConnection(false);
        Query query = connection.parseQuery(queryString);
        Result result = connection.execute(query);
        return result;
    }

    /** Executes a query against the FoodMart database, and returns the
     * exception, or <code>null</code> if there was no exception. **/
    public Throwable executeFoodMartCatch(String queryString) {
        try {
            Result result = executeFoodMart(queryString);
            mondrian.olap.Util.discard(result);
        } catch (Throwable e) {
            return e;
        }
        return null;
    }

    /** Returns the output writer. **/
    public PrintWriter getWriter() {
        return pw;
    }
}

// End TestContext.java
