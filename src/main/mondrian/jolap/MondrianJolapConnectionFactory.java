/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Dec 23, 2002
*/
package mondrian.jolap;

import mondrian.olap.DriverManager;
import mondrian.olap.Hierarchy;
import mondrian.olap.Util;
import mondrian.util.BarfingInvocationHandler;

import javax.jmi.reflect.RefPackage;
import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;
import javax.olap.OLAPException;
import javax.olap.metadata.Schema;
import javax.olap.query.querycoremodel.CubeView;
import javax.olap.query.querycoremodel.DimensionView;
import javax.olap.query.querycoremodel.EdgeView;
import javax.olap.query.querycoremodel.MeasureView;
import javax.olap.resource.*;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

/**
 * <code>MondrianJolapConnectionFactory</code> creates JOLAP connections to
 * Mondrian.
 *
 * @author jhyde
 * @since Dec 23, 2002
 * @version $Id$
 **/
public class MondrianJolapConnectionFactory extends RefObjectSupport
		implements ConnectionFactory, InitialContextFactory {
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
		propertyList.put("Provider", "mondrian");
		propertyList.put("User", properties.getName());
		propertyList.put("Password", properties.getPassword());
		propertyList.put("Jdbc", "jdbc:odbc:MondrianFoodMart");
		propertyList.put("Catalog", "file:///e:/mondrian/demo/FoodMart.xml");
		propertyList.put("JdbcDrivers", "sun.jdbc.odbc.JdbcOdbcDriver,oracle.jdbc.OracleDriver,com.mysql.jdbc.Driver");
		mondrian.olap.Connection mondrianConnection =
				DriverManager.getConnection(propertyList, true);
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

	MondrianJolapConnectionSpec() {
		propertyList.put("Provider", "mondrian");
		propertyList.put("Jdbc", "jdbc:odbc:MondrianFoodMart");
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

class MondrianJolapConnection extends RefObjectSupport implements Connection {
	private mondrian.olap.Connection connection;

	MondrianJolapConnection(mondrian.olap.Connection connection) {
		this.connection = connection;
	}
	public void close() throws OLAPException {
		connection.close();
	}

	public ConnectionMetaData getMetaData() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public RefPackage getTopLevelPackage() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public List getSchema() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public Schema getCurrentSchema() throws OLAPException {
		return null; // todo:
	}

	public void setCurrentSchema(Schema schema) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public List getDimensions() throws OLAPException {
		final Hierarchy[] sharedHierarchies = connection.getSchema().getSharedHierarchies();
		final ArrayList list = new ArrayList();
		for (int i = 0; i < sharedHierarchies.length; i++) {
			list.add(new MondrianJolapDimension(getCurrentSchema(), sharedHierarchies[i].getDimension()));
		}
		return list;
	}

	public List getCubes() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public CubeView createCubeView() throws OLAPException {
		return new MondrianCubeView();
	}

	public DimensionView createDimensionView() throws OLAPException {
		return new MondrianDimensionView();
	}

	public EdgeView createEdgeView() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public MeasureView createMeasureView() throws OLAPException {
		return new MondrianDimensionView();
	}
}

// End MondrianJolapConnectionFactory.java