/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2003-2005 Julian Hyde
// Copyright (C) 2005-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap;

/**
 * <code>RolapConnectionProperties</code> enumerates the allowable values of
 * keywords in a Mondrian connect string.
 *
 * <p><b>Note to developers</b>: If you add or modify a connection-string
 * property, you must also modify the
 * <a target="_top"
 * href="{@docRoot}/../configuration.html#Connect_string_properties">
 * Configuration Specification</a>.
 *
 * @author jhyde, Mar 18, 2003
 */
public enum RolapConnectionProperties {
    /**
     * The "Provider" property must have the value <code>"Mondrian"</code>.
     */
    Provider,

    /**
     * The "Jdbc" property is the URL of the JDBC database where the data is
     * stored. You must specify either {@link #DataSource} or {@code Jdbc}.
     */
    Jdbc,

    /**
     * The "JdbcDrivers" property is a comma-separated list of JDBC driver
     * classes, for example, <code>"com.mysql.jdbc.Driver,
     * sun.jdbc.odbc.JdbcOdbcDriver, oracle.jdbc.OracleDriver"</code>.
     */
    JdbcDrivers,

    /**
     * The "JdbcUser" property is the name of the user to log on to the JDBC
     * database. (You don't need to specify this parameter if it is already
     * specified in the JDBC URL.)
     */
    JdbcUser,

    /**
     * The "JdbcPassword" property is the password to log on to the JDBC
     * database. (You don't need to specify this parameter if it is already
     * specified in the JDBC URL.)
     */
    JdbcPassword,

    /**
     * The "Dialect" property specifies the SQL dialect to use for connections
     * to this JDBC database.
     *
     * <p>The value of the property is the name of a class that implements the
     * {@link mondrian.spi.Dialect} SPI. The class must be on the classpath. It
     * is recommended, but not essential, that the class is in the
     * <code>META-INF/services/mondrian.spi.Dialect</code> configuration
     * file.</p>
     *
     * <p>If the property is not specified, Mondrian scans the registered
     * dialects and uses the first that claims to be able to handle this kind
     * of JDBC connection.</p>
     */
    Dialect,

    /**
     * The "Catalog" property is the URL of the catalog, an XML file which
     * describes the schema: cubes, hierarchies, and so forth.
     * Catalogs are described in <a target="_top"
     * href="{@docRoot}/../schema.html">the Schema Guide</a>.
     * See also {@link #CatalogContent}.
     */
    Catalog,

    /**
     * The "CatalogContent" property is an XML string representing the schema:
     * cubes, hierarchies, and so forth.
     * Catalogs are described in <a target="_top"
     * href="{@docRoot}/../schema.html">the Schema Guide</a>.
     *
     * <p>When using this property, quote its value with either single or
     * double quotes, then escape all occurrences of that character within the
     * catalog content by using double single/double quotes. ie:
     *
     * <p>&nbsp;&nbsp;CatalogContent="&lt;Schema name=""My Schema""/&gt;"
     *
     * <p>See also {@link #Catalog}.
     */
    CatalogContent,

    /**
     * The "CatalogName" property is not used. If, in future, we support
     * multiple catalogs, this property will specify which catalog to use.
     * See also {@link #Catalog}.
     */
    CatalogName,

    /**
     * The "DataSource" property is the name of a data source class. It must
     * implement the {@link javax.sql.DataSource} interface.
     * You must specify either {@code DataSource} or {@link #Jdbc}.
     */
    DataSource,

    /**
     * The "PoolNeeded" property tells Mondrian whether to add a layer of
     * connection pooling.
     *
     * <p>If the value is "true", Mondrian uses its own connection pool
     * for all JDBC connections.</p>
     *
     * <p>If the value is "false", Mondrian does not use a connection pool
     * for any JDBC connections.</p>
     *
     * <p>If no value is specified, Mondrian uses a connection pool for
     * connections created via the JDBC driver manager (that is, using the
     * {@link #Jdbc} property. In other words, it assumes that:<ul>
     * <li>connections created via the {@link #Jdbc} property are not pooled,
     *     and therefore need to be pooled,
     * <li>connections created via the {@link #DataSource} are already pooled.
     * </ul>
     */
    PoolNeeded,

    /**
     * The "Role" property is the name of the {@link mondrian.olap.Role role}
     * to adopt. If not specified, the connection uses a role which has access
     * to every object in the schema.
     */
    Role,

    /**
     * Allows to work with dynamically changing schema. If this property is set
     * to <code>true</code> and schema content has changed (previous checksum
     * doesn't equal with current), schema would be reloaded. Could be used in
     * combination with <code>DynamicSchemaProcessor</code> property
     */
    UseContentChecksum,

    /**
     * The "UseSchemaPool" property disables the schema cache. If false, the
     * schema is not shared with connections which have a textually identical
     * schema. Default is "true".
     */
    UseSchemaPool,

    /**
     * The name of a class implementing the
     * {@link mondrian.spi.DynamicSchemaProcessor} interface.
     * A dynamic schema processor is called at runtime in order to modify the
     * schema content.
     */
    DynamicSchemaProcessor,

    /**
     * The "Locale" property is the requested Locale for the
     * LocalizingDynamicSchemaProcessor.  Example values are "en",
     * "en_US", "hu". If Locale is not specified, then the name of system's
     * default will be used, as per {@link java.util.Locale#getDefault()}.
     */
    Locale,

    /**
     * The "Ignore" property is a boolean value. If true, mondrian ignores
     * warnings and non-fatal errors while loading the schema. The resulting
     * errors can be obtained by calling
     * {@link mondrian.olap.Schema#getWarnings}.
     */
    Ignore,

    /**
     * The "Instance" property is the unique identifier of a mondrian server
     * running in the current JVM. If there are multiple mondrian servers, it
     * ensures that the connection belongs to the correct one.
     */
    Instance,

    /**
     * The "JdbcConnectionUuid" property is the unique identifier for the
     * underlying JDBC connection. If defined, Mondrian will assume that two
     * connections bearing the same JdbcConnectionUuid point to identical
     * databases without looking at any other properties.
     */
    JdbcConnectionUuid,

    /**
     * The "DataServicesProvider" property specifies the full class name for
     * the {@link mondrian.spi.DataServicesProvider} implementation to be used
     * for this connection
     */
    DataServicesProvider;

    /**
     * Any property beginning with this value will be added to the
     * JDBC connection properties, after removing this prefix. This
     * allows you to specify connection properties without a URL.
     */
    public static final String JdbcPropertyPrefix = "jdbc.";

    /**
     * Any property beginning with this value will be added to the
     * map passed to a {@link mondrian.spi.RoleGenerator}.
     */
    public static final String RolePropertyPrefix = "session.";
}

// End RolapConnectionProperties.java
