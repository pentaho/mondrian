/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Dec 23, 2002
*/
package mondrian.jolap;

import mondrian.olap.DriverManager;
import mondrian.olap.Util;
import mondrian.util.BarfingInvocationHandler;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.spi.InitialContextFactory;
import javax.olap.OLAPException;
import javax.olap.resource.Connection;
import javax.olap.resource.ConnectionFactory;
import javax.olap.resource.ConnectionSpec;
import javax.olap.resource.ResourceAdapterMetaData;
import java.lang.reflect.Proxy;
import java.util.Hashtable;

/**
 * Factory for JOLAP connections to a Mondrian database.
 *
 * @author jhyde
 * @since Dec 23, 2002
 * @version $Id$
 **/
public class MondrianJolapConnectionFactory extends RefObjectSupport
        implements ConnectionFactory, InitialContextFactory {

    private static final String FOODMART_CATALOG_URL = "mondrian.test.foodmart.catalogURL";

    /** Creates a <code>MondrianJolapConnectionFactory</code>. Generally, the
     * driver manager will call this, as long as you provide it with the name
     * of this class. **/
    public MondrianJolapConnectionFactory() {
    }

    public Connection getConnection() throws OLAPException {
        throw new UnsupportedOperationException();
    }

    public Connection getConnection(ConnectionSpec properties) throws OLAPException {
        Util.PropertyList propertyList = new Util.PropertyList();
        MondrianJolapConnectionSpec cs = (MondrianJolapConnectionSpec) properties;
        propertyList.put("Provider", "mondrian");
        propertyList.put("User", cs.getName());
        propertyList.put("Password", cs.getPassword());
        propertyList.put("Jdbc", "jdbc:odbc:MondrianFoodMart");
        if( System.getProperty(FOODMART_CATALOG_URL) != null )
            propertyList.put("Catalog", System.getProperty(FOODMART_CATALOG_URL));
        else
            propertyList.put("Catalog", "file:///e:/mondrian/demo/FoodMart.xml");
        propertyList.put("JdbcDrivers", "sun.jdbc.odbc.JdbcOdbcDriver,oracle.jdbc.OracleDriver,com.mysql.jdbc.Driver");
        final boolean fresh = false;
        mondrian.olap.Connection mondrianConnection = DriverManager.
                getConnection(propertyList, null, fresh);
        return new MondrianJolapConnection(mondrianConnection);
    }

    public ConnectionSpec createConnectionSpec() throws OLAPException {
        return new MondrianJolapConnectionSpec();
    }

    public ResourceAdapterMetaData getMetaData() throws OLAPException {
        throw new UnsupportedOperationException();
    }

    public Context getInitialContext(Hashtable environment)
            throws NamingException {
        return (Context) Proxy.newProxyInstance(
                null,
                new Class[] {Context.class},
                new ContextHandler()
        );
    }

    public void setReference(Reference ref) {
        throw new UnsupportedOperationException();
    }

    public Reference getReference() throws NamingException {
        throw new UnsupportedOperationException();
    }

    public class ContextHandler extends BarfingInvocationHandler {
        public Object lookup(String name) throws NamingException {
            Util.assertTrue(name.equals("JOLAPServer"));
            return MondrianJolapConnectionFactory.this;
        }
    }
}

/**
 * <code>MondrianJolapConnectionSpec</code> contains the attributes necessary
 * to establish a JOLAP connection to a Mondrian server.
 *
 * @see MondrianJolapConnectionFactory#createConnectionSpec
 */
class MondrianJolapConnectionSpec implements ConnectionSpec {
    Util.PropertyList propertyList = new Util.PropertyList();
    private static final String FOODMART_CATALOG_URL = "mondrian.test.foodmart.catalogURL";

    MondrianJolapConnectionSpec() {
        propertyList.put("Provider", "mondrian");
        propertyList.put("Jdbc", "jdbc:odbc:MondrianFoodMart");
        if( System.getProperty(FOODMART_CATALOG_URL) != null )
            propertyList.put("Catalog", System.getProperty(FOODMART_CATALOG_URL));
        else
            propertyList.put("Catalog", "file:///e:/mondrian/demo/FoodMart.xml");
        propertyList.put("JdbcDrivers", "sun.jdbc.odbc.JdbcOdbcDriver,oracle.jdbc.OracleDriver,com.mysql.jdbc.Driver");
    }
    public void setName(String name) {
        propertyList.put("name", name);
    }
    public String getName() {
        return propertyList.get("name");
    }
    public void setPassword(String password) {
        propertyList.put("password", password);
    }
    public String getPassword() {
        return propertyList.get("password");
    }
    public void setCatalog(String catalog) {
        propertyList.put("catalog", catalog);
    }
    public String getCatalog() {
        return propertyList.get("catalog");
    }
    public void setJdbc(String jdbc) {
        propertyList.put("jdbc", jdbc);
    }
    public String getJdbc() {
        return propertyList.get("jdbc");
    }
    public void setJdbcDrivers(String jdbcDrivers) {
        propertyList.put("jdbcDrivers", jdbcDrivers);
    }
    public String getJdbcDrivers() {
        return propertyList.get("jdbcDrivers");
    }
}

// End MondrianJolapConnectionFactory.java
