/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2003-2003 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Mar 18, 2003
*/
package mondrian.rolap;

import mondrian.olap.EnumeratedValues;

/**
 * <code>RolapConnectionProperties</code> enumerates the allowable values of
 * keywords in a Mondrian connect string.
 */
public class RolapConnectionProperties extends EnumeratedValues {
	public static final RolapConnectionProperties instance = new RolapConnectionProperties();

	private RolapConnectionProperties() {
		super(new String[] {
			Provider, Jdbc, JdbcDrivers, JdbcUser, JdbcPassword, Catalog,
			CatalogContent, DataSource, PoolNeeded, Role});
	}
	/**
	 * @{value} must equal <code>"Mondrian"</code>.
	 */
	public static final String Provider = "Provider";
	/**
	 * @{value} is the URL of the JDBC database where the data is stored.
	 * You must specify either {@link #DataSource} or {@link #Jdbc}.
	 */
	public static final String Jdbc = "Jdbc";
	/**
	 * @{value} is a comma-separated list of JDBC driver classes, for
	 * example, <code>"sun.jdbc.odbc.JdbcOdbcDriver,oracle.jdbc.OracleDriver"</code>.
	 */
	public static final String JdbcDrivers = "JdbcDrivers";
	/**
	 * @{value} is the name of the user to log on to the JDBC database. (You
	 * don't need to specify this parameter if it is already specified in
	 * the JDBC URL.)
	 */
	public static final String JdbcUser = "JdbcUser";
	/**
	 * @{value} is the password to log on to the JDBC database. (You
	 * don't need to specify this parameter if it is already specified in
	 * the JDBC URL.)
	 */
	public static final String JdbcPassword = "JdbcPassword";
	/**
	 * @{value} is the URL of the catalog, an XML file which describes
	 * the schema: cubes, hierarchies, and so forth. Catalogs are described
	 * <a target="_top" href="http://apoptosis.dyndns.org:8080/open/mondrian/doc/schema.html">here</a>.
	 * See also {@link #CatalogContent}.
	 */
	public static final String Catalog = "Catalog";
	/**
	 * @{value} is an XML string representing
	 * the schema: cubes, hierarchies, and so forth. Catalogs are described
	 * <a target="_top" href="http://apoptosis.dyndns.org:8080/open/mondrian/doc/schema.html">here</a>.
	 * See also {@link #Catalog}.
	 */
	public static final String CatalogContent = "CatalogContent";
	/**
	 * @{value} is the name of a data source class. It must implement
	 * {@link javax.sql.DataSource}.
	 * You must specify either {@link #DataSource} or {@link #Jdbc}.
	 */
	public static final String DataSource = "DataSource";
    /**
     * @{value} tells Mondrian whether to add a layer of connection pooling.
     * 
     * <p>If no value is specified, we assume that:<ul>
     * <li>connections created via the {@link #Jdbc} property are not pooled,
     *     and therefore need to be pooled,
     * <li>connections created via the {@link #DataSource} are already pooled.
     * </ul>
     */
    public static final String PoolNeeded = "PoolNeeded";
	/**
	 * {@value} is the name of the {@link mondrian.olap.Role role} to adopt. If
	 * not specified, the connection uses a role which has access to every
	 * object in the schema.
	 */
	public static final String Role = "Role";
}

// End RolapConnectionProperties.java