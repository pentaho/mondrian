/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2003-2005 Julian Hyde
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
    public static final RolapConnectionProperties instance =
        new RolapConnectionProperties();

    private RolapConnectionProperties() {
        super(new String[] {
            Provider, Jdbc, JdbcDrivers, JdbcUser, JdbcPassword, Catalog,
            Locale, CatalogContent, CatalogName, DataSource, PoolNeeded, Role,
            DynamicSchemaProcessor});
    }

    /**
     * The "Provider" property must have the value <code>"Mondrian"</code>.
     */
    public static final String Provider = "Provider";

    /**
     * The "Jdbc" property is the URL of the JDBC database where the data is
     * stored. You must specify either {@link #DataSource} or {@link #Jdbc}.
     */
    public static final String Jdbc = "Jdbc";

    /**
     * The "JdbcDrivers" property is a comma-separated list of JDBC driver
     * classes, for example,
     * <code>"sun.jdbc.odbc.JdbcOdbcDriver,oracle.jdbc.OracleDriver"</code>.
     */
    public static final String JdbcDrivers = "JdbcDrivers";

    /**
     * The "JdbcUser" property is the name of the user to log on to the JDBC
     * database. (You don't need to specify this parameter if it is already
     * specified in the JDBC URL.)
     */
    public static final String JdbcUser = "JdbcUser";

    /**
     * The "JdbcPassword" property is the password to log on to the JDBC
     * database. (You don't need to specify this parameter if it is already
     * specified in the JDBC URL.)
     */
    public static final String JdbcPassword = "JdbcPassword";

    /**
     * The "Catalog" property is the URL of the catalog, an XML file which
     * describes the schema: cubes, hierarchies, and so forth.
     * Catalogs are described in <a target="_top"
     * href="{@docRoot}/../schema.html">the Schema Guide</a>.
     * See also {@link #CatalogContent}.
     */
    public static final String Catalog = "Catalog";

    /**
     * The "CatalogContent" property is an XML string representing the schema:
     * cubes, hierarchies, and so forth.
     * Catalogs are described in <a target="_top"
     * href="{@docRoot}/../schema.html">the Schema Guide</a>.
     * See also {@link #Catalog}.
     */
    public static final String CatalogContent = "CatalogContent";

    /**
     * The "CatalogName" property is not used. If, in future, we support
     * multiple catalogs, this property will specify which catalog to use.
     * See also {@link #Catalog}.
     */
    public static final String CatalogName = "CatalogName";

    /**
     * The "DataSource" property is the name of a data source class. It must
     * implement the {@link javax.sql.DataSource} interface.
     * You must specify either {@link #DataSource} or {@link #Jdbc}.
     */
    public static final String DataSource = "DataSource";

    /**
     * The "PoolNeeded" property tells Mondrian whether to add a layer of
     * connection pooling.
     *
     * <p>If no value is specified, we assume that:<ul>
     * <li>connections created via the {@link #Jdbc} property are not pooled,
     *     and therefore need to be pooled,
     * <li>connections created via the {@link #DataSource} are already pooled.
     * </ul>
     */
    public static final String PoolNeeded = "PoolNeeded";

    /**
     * The "Role" property is the name of the {@link mondrian.olap.Role role}
     * to adopt. If not specified, the connection uses a role which has access
     * to every object in the schema.
     */
    public static final String Role = "Role";

    /**
     * Any property beginning with this value will be added to the
     * JDBC connection properties, after removing this prefix. This
     * allows you to specify connection properties without a URL.
     */
    public static final String JdbcPropertyPrefix = "jdbc.";

    /**
     * The name of a class implementing mondrian.rolap.DynamicSchemaProcessor.
     * A dynamic schema prozessor is called at runtime in order to modify the
     * schema content.
     */
    public static final String DynamicSchemaProcessor = "DynamicSchemaProcessor";

    /**
     * The "Locale" property is the requested Locale for the
     * LocalizingDynamicSchemaProcessor.  Example values are "en",
     * "en_US", "hu". If Locale is not specified, then the name of system's
     * default will be used, as per {@link java.util.Locale#getDefault()}.
     */
    public static final String Locale = "Locale";
}

// End RolapConnectionProperties.java
