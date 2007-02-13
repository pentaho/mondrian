/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 2 October, 2002
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.util.MemoryMonitor;
import mondrian.util.MemoryMonitorFactory;
import mondrian.rolap.agg.AggregationManager;
import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DataSourceConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;

import org.apache.log4j.Logger;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

/**
 * A <code>RolapConnection</code> is a connection to a Mondrian OLAP Server.
 *
 * <p>Typically, you create a connection via
 * {@link DriverManager#getConnection(String, mondrian.spi.CatalogLocator, boolean)}.
 * {@link RolapConnectionProperties} describes allowable keywords.</p>
 *
 * @see RolapSchema
 * @see DriverManager
 * @author jhyde
 * @since 2 October, 2002
 * @version $Id$
 */
public class RolapConnection extends ConnectionBase {
    private static final Logger LOGGER = Logger.getLogger(RolapConnection.class);

    private final Util.PropertyList connectInfo;

    /**
     * Factory for JDBC connections to talk to the RDBMS. This factory will
     * usually use a connection pool.
     */
    private final DataSource dataSource;
    private final String catalogUrl;
    private final RolapSchema schema;
    private SchemaReader schemaReader;
    protected Role role;
    private Locale locale = Locale.US;

    /**
     * Creates a connection.
     *
     * @param connectInfo Connection properties; keywords are described in
     *   {@link RolapConnectionProperties}.
     */
    public RolapConnection(Util.PropertyList connectInfo) {
        this(connectInfo, null, null);
    }

    /**
     * Creates a connection.
     *
     * @param connectInfo Connection properties; keywords are described in
     *   {@link RolapConnectionProperties}.
     */
    public RolapConnection(Util.PropertyList connectInfo, DataSource datasource) {
        this(connectInfo, null, datasource);
    }

    /**
     * Creates a RolapConnection.
     *
     * <p>Only {@link mondrian.rolap.RolapSchema.Pool#get} calls this with schema != null (to
     * create a schema's internal connection). Other uses retrieve a schema
     * from the cache based upon the <code>Catalog</code> property.
     *
     * @param connectInfo Connection properties; keywords are described in
     *   {@link RolapConnectionProperties}.
     * @param schema Schema for the connection. Must be null unless this is to
     *   be an internal connection.
     * @pre connectInfo != null
     */
    RolapConnection(Util.PropertyList connectInfo, RolapSchema schema) {
        this(connectInfo, schema, null);
    }

    /**
     * Creates a RolapConnection.
     *
     * <p>Only {@link mondrian.rolap.RolapSchema.Pool#get} calls this with
     * schema != null (to create a schema's internal connection).
     * Other uses retrieve a schema from the cache based upon
     * the <code>Catalog</code> property.
     *
     * @param connectInfo Connection properties; keywords are described in
     *   {@link RolapConnectionProperties}.
     * @param schema Schema for the connection. Must be null unless this is to
     *   be an internal connection.
     * @param dataSource If not null an external DataSource to be used
     *        by Mondrian
     * @pre connectInfo != null
     */
    RolapConnection(
        Util.PropertyList connectInfo,
        RolapSchema schema,
        DataSource dataSource)
    {
        super();

        String provider = connectInfo.get(
            RolapConnectionProperties.Provider.name(), "mondrian");
        Util.assertTrue(provider.equalsIgnoreCase("mondrian"));
        this.connectInfo = connectInfo;
        this.catalogUrl =
            connectInfo.get(RolapConnectionProperties.Catalog.name());
        this.dataSource = (dataSource != null)
            ? dataSource
            : createDataSource(connectInfo);
        Role role = null;
        if (schema == null) {
            // If RolapSchema.Pool.get were to call this with schema == null,
            // we would loop.
            if (dataSource == null) {
                // If there is no external data source is passed in,
                // we expect the following properties to be set,
                // as they are used to generate the schema cache key.
                final String jdbcConnectString =
                    connectInfo.get(RolapConnectionProperties.Jdbc.name());
                final String jdbcUser =
                    connectInfo.get(RolapConnectionProperties.JdbcUser.name());
                final String strDataSource =
                    connectInfo.get(RolapConnectionProperties.DataSource.name());
                final String connectionKey = jdbcConnectString +
                getJDBCProperties(connectInfo).toString();

                schema = RolapSchema.Pool.instance().get(
                    catalogUrl,
                    connectionKey,
                    jdbcUser,
                    strDataSource,
                    connectInfo);
            } else {
                schema = RolapSchema.Pool.instance().get(
                    catalogUrl,
                    dataSource,
                    connectInfo);
            }
            String roleName =
                connectInfo.get(RolapConnectionProperties.Role.name());
            if (roleName != null) {
                role = schema.lookupRole(roleName);
                if (role == null) {
                    throw Util.newError("Role '" + roleName + "' not found");
                }
            }
        }
        if (role == null) {
            role = schema.getDefaultRole();
        }

        // Set the locale.
        String localeString =
            connectInfo.get(RolapConnectionProperties.Locale.name());
        if (localeString != null) {
            String[] strings = localeString.split("_");
            switch (strings.length) {
            case 1:
                this.locale = new Locale(strings[0]);
                break;
            case 2:
                this.locale = new Locale(strings[0], strings[1]);
                break;
            case 3:
                this.locale = new Locale(strings[0], strings[1], strings[2]);
                break;
            default:
                throw Util.newInternal("bad locale string '" + localeString + "'");
            }
        }

        this.schema = schema;
        setRole(role);
    }

    protected Logger getLogger() {
        return LOGGER;
    }


    // This is package-level in order for the RolapConnectionTest class to have
    // access.
    static DataSource createDataSource(Util.PropertyList connectInfo) {
        final String jdbcConnectString =
                connectInfo.get(RolapConnectionProperties.Jdbc.name());
        final String poolNeededString =
                connectInfo.get(RolapConnectionProperties.PoolNeeded.name());

        Properties jdbcProperties = getJDBCProperties(connectInfo);
        String propertyString = jdbcProperties.toString();
        if (jdbcConnectString != null) {
            // Get connection through own pooling datasource
            String jdbcDrivers =
                connectInfo.get(RolapConnectionProperties.JdbcDrivers.name());
            if (jdbcDrivers != null) {
                RolapUtil.loadDrivers(jdbcDrivers);
            }
            final String jdbcDriversProp =
                    MondrianProperties.instance().JdbcDrivers.get();
            RolapUtil.loadDrivers(jdbcDriversProp);

            final boolean poolNeeded = (poolNeededString == null)
                // JDBC connections are dumb beasts, so we assume they're not
                // pooled.
                ? true
                : poolNeededString.equalsIgnoreCase("true");

            final String jdbcUser =
                connectInfo.get(RolapConnectionProperties.JdbcUser.name());
            final String jdbcPassword =
                connectInfo.get(RolapConnectionProperties.JdbcPassword.name());

            if (jdbcUser != null) {
                jdbcProperties.put("user", jdbcUser);
            }
            if (jdbcPassword != null) {
                jdbcProperties.put("password", jdbcPassword);
            }

            if (!poolNeeded) {
                // Connection is already pooled; don't pool it again.
                return new DriverManagerDataSource(jdbcConnectString,
                        jdbcProperties);
            }

            if (jdbcConnectString.toLowerCase().indexOf("mysql") > -1) {
                // mysql driver needs this autoReconnect parameter
                jdbcProperties.setProperty("autoReconnect", "true");
            }
            // use the DriverManagerConnectionFactory to create connections
            ConnectionFactory connectionFactory =
                    new DriverManagerConnectionFactory(jdbcConnectString ,
                            jdbcProperties);
            try {
                return RolapConnectionPool.instance().getPoolingDataSource(
                        jdbcConnectString + propertyString, connectionFactory);
            } catch (Throwable e) {
                throw Util.newInternal(e,
                        "Error while creating connection pool (with URI " +
                        jdbcConnectString + ")");
            }

        } else {

            final String dataSourceName =
                connectInfo.get(RolapConnectionProperties.DataSource.name());
            if (dataSourceName == null) {
                throw Util.newInternal(
                    "Connect string '" + connectInfo.toString() +
                    "' must contain either '" + RolapConnectionProperties.Jdbc +
                    "' or '" + RolapConnectionProperties.DataSource + "'");
            }

            final boolean poolNeeded = (poolNeededString == null)
                // Data sources are fairly smart, so we assume they look after
                // their own pooling.
                ? false
                : poolNeededString.equalsIgnoreCase("true");

            // Get connection from datasource.
            final DataSource dataSource;
            try {
                dataSource =
                    (DataSource) new InitialContext().lookup(dataSourceName);
            } catch (NamingException e) {
                throw Util.newInternal(e,
                    "Error while looking up data source (" +
                        dataSourceName + ")");
            }
            if (!poolNeeded) {
                return dataSource;
            }
            ConnectionFactory connectionFactory =
                    new DataSourceConnectionFactory(dataSource);
            try {
                return RolapConnectionPool.instance().getPoolingDataSource(
                        dataSourceName, connectionFactory);
            } catch (Exception e) {
                throw Util.newInternal(e,
                        "Error while creating connection pool (with URI " +
                        dataSourceName + ")");
            }
        }
    }

    /**
     * Creates a {@link Properties} object containing all of the JDBC
     * connection properties present in the
     * {@link Util.PropertyList connectInfo}.
     *
     * @param connectInfo
     * @return The JDBC connection properties.
     */
    private static Properties getJDBCProperties(Util.PropertyList connectInfo) {
        Properties jdbcProperties = new Properties();
        Iterator<String[]> iterator = connectInfo.iterator();
        while (iterator.hasNext()) {
            String[] entry = iterator.next();
            if (entry[0].startsWith(RolapConnectionProperties.JdbcPropertyPrefix)) {
                jdbcProperties.put(entry[0].substring(RolapConnectionProperties.JdbcPropertyPrefix.length()), entry[1]);
            }
        }
        return jdbcProperties;
    }

    public Util.PropertyList getConnectInfo() {
        return connectInfo;
    }

    public void close() {
    }

    public Schema getSchema() {
        return schema;
    }

    public String getConnectString() {
        return connectInfo.toString();
    }

    public String getCatalogName() {
        return catalogUrl;
    }

    public Locale getLocale() {
        return locale;
    }

    public void setLocale(Locale locale) {
        this.locale = locale;
    }

    public SchemaReader getSchemaReader() {
        return schemaReader;
    }

    public Object getProperty(String name) {
        // Mask out the values of certain properties.
        if (name.equals(RolapConnectionProperties.JdbcPassword.name()) ||
            name.equals(RolapConnectionProperties.CatalogContent.name())) {
            return "";
        }
        return connectInfo.get(name);
    }

    public CacheControl getCacheControl(PrintWriter pw) {
        return AggregationManager.instance().getCacheControl(pw);
    }

    /**
     * Executes a Query.
     *
     * @throws ResourceLimitExceededException if some resource limit specified in the
     * property file was exceeded
     * @throws QueryCanceledException if query was canceled during execution
     * @throws QueryTimeoutException if query exceeded timeout specified in
     * the property file
     */
    public Result execute(Query query) {
        class Listener implements MemoryMonitor.Listener {
            private final Query query;
            Listener(final Query query) {
                this.query = query;
            }
            public void memoryUsageNotification(long used, long max) {
                StringBuilder buf = new StringBuilder(200);
                buf.append("OutOfMemory used=");
                buf.append(used);
                buf.append(", max=");
                buf.append(max);
                buf.append(" for connection: ");
                buf.append(getConnectString());
                // Call ConnectionBase method which has access to
                // Query methods.
                RolapConnection.memoryUsageNotification(query, buf.toString());
            }
        }
        Listener listener = new Listener(query);
        MemoryMonitor mm = MemoryMonitorFactory.instance().getObject();
        try {
            mm.addListener(listener);
            // Check to see if we must punt
            query.checkCancelOrTimeout();

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(Util.unparse(query));
            }
            query.setQueryStartTime();
            Result result = new RolapResult(query, true);
            for (int i = 0; i < query.axes.length; i++) {
                QueryAxis axis = query.axes[i];
                if (axis.isNonEmpty()) {
                    result = new NonEmptyResult(result, query, i);
                }
            }
            if (LOGGER.isDebugEnabled()) {
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                result.print(pw);
                pw.flush();
                LOGGER.debug(sw.toString());
            }
            query.setQueryEndExecution();
            return result;

        } catch (ResultLimitExceededException e) {
            // query has been punted
            throw e;
        } catch (Exception e) {
            String queryString;
            query.setQueryEndExecution();
            try {
                queryString = Util.unparse(query);
            } catch (Exception e1) {
                queryString = "?";
            }
            throw Util.newError(e, "Error while executing query [" +
                    queryString + "]");
        } finally {
            mm.removeListener(listener);
        }
    }

    public void setRole(Role role) {
        assert role != null;
        assert !role.isMutable();

        this.role = role;
        this.schemaReader = new RolapSchemaReader(role, schema) {
            public Cube getCube() {
                throw new UnsupportedOperationException();
            }
        };
    }

    public Role getRole() {
        Util.assertPostcondition(role != null, "role != null");
        Util.assertPostcondition(!role.isMutable(), "!role.isMutable()");

        return role;
    }

    /**
     * Implementation of {@link DataSource} which calls the good ol'
     * {@link java.sql.DriverManager}.
     */
    private static class DriverManagerDataSource implements DataSource {
        private final String jdbcConnectString;
        private PrintWriter logWriter;
        private int loginTimeout;
        private Properties jdbcProperties;

        public DriverManagerDataSource(String jdbcConnectString,
                                       Properties properties) {
            this.jdbcConnectString = jdbcConnectString;
            this.jdbcProperties = properties;
        }

        public Connection getConnection() throws SQLException {
            return new org.apache.commons.dbcp.DelegatingConnection(
                java.sql.DriverManager.getConnection(
                    jdbcConnectString, jdbcProperties));
        }

        public Connection getConnection(String username, String password)
                throws SQLException {
            if (jdbcProperties == null) {
                return java.sql.DriverManager.getConnection(jdbcConnectString,
                        username, password);
            } else {
                Properties temp = (Properties)jdbcProperties.clone();
                temp.put("user", username);
                temp.put("password", password);
                return java.sql.DriverManager.getConnection(jdbcConnectString, temp);
            }
        }

        public PrintWriter getLogWriter() throws SQLException {
            return logWriter;
        }

        public void setLogWriter(PrintWriter out) throws SQLException {
            logWriter = out;
        }

        public void setLoginTimeout(int seconds) throws SQLException {
            loginTimeout = seconds;
        }

        public int getLoginTimeout() throws SQLException {
            return loginTimeout;
        }

        public <T> T unwrap(Class<T> iface) throws SQLException {
            throw new SQLException("not a wrapper");
        }

        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return false;
        }
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    /**
     * A <code>NonEmptyResult</code> filters a result by removing empty rows
     * on a particular axis.
     */
    class NonEmptyResult extends ResultBase {

        final Result underlying;
        private final int axis;
        private final Map<Integer, Integer> map;
        /** workspace. Synchronized access only. */
        private final int[] pos;

        NonEmptyResult(Result result, Query query, int axis) {
            super(query, result.getAxes().clone());

            this.underlying = result;
            this.axis = axis;
            this.map = new HashMap<Integer, Integer>();
            int axisCount = underlying.getAxes().length;
            this.pos = new int[axisCount];
            this.slicerAxis = underlying.getSlicerAxis();
            List<Position> positions = underlying.getAxes()[axis].getPositions();

            List<Position> positionsList = new ArrayList<Position>();
            int i = 0;
            for (Position position: positions) {
                if (! isEmpty(i, axis)) {
                    map.put(positionsList.size(), i);
                    positionsList.add(position);
                }
                i++;
            }
            this.axes[axis] = new RolapAxis.PositionList(positionsList);
        }

        protected Logger getLogger() {
            return LOGGER;
        }

        /**
         * Returns true if all cells at a given offset on a given axis are
         * empty. For example, in a 2x2x2 dataset, <code>isEmpty(1,0)</code>
         * returns true if cells <code>{(1,0,0), (1,0,1), (1,1,0),
         * (1,1,1)}</code> are all empty. As you can see, we hold the 0th
         * coordinate fixed at 1, and vary all other coordinates over all
         * possible values.
         */
        private boolean isEmpty(int offset, int fixedAxis) {
            int axisCount = getAxes().length;
            pos[fixedAxis] = offset;
            return isEmptyRecurse(fixedAxis, axisCount - 1);
        }

        private boolean isEmptyRecurse(int fixedAxis, int axis) {
            if (axis < 0) {
                RolapCell cell = (RolapCell) underlying.getCell(pos);
                return cell.isNull();
            } else if (axis == fixedAxis) {
                return isEmptyRecurse(fixedAxis, axis - 1);
            } else {
                List<Position> positions = getAxes()[axis].getPositions();
                int i = 0;
                for (Position position: positions) {
                    pos[axis] = i;
                    if (!isEmptyRecurse(fixedAxis, axis - 1)) {
                        return false;
                    }
                    i++;
                }
                return true;
            }
        }

        // synchronized because we use 'pos'
        public synchronized Cell getCell(int[] externalPos) {
            System.arraycopy(externalPos, 0, this.pos, 0, externalPos.length);
            int offset = externalPos[axis];
            int mappedOffset = mapOffsetToUnderlying(offset);
            this.pos[axis] = mappedOffset;
            return underlying.getCell(this.pos);
        }

        private int mapOffsetToUnderlying(int offset) {
            return map.get(offset);
        }

        public void close() {
            underlying.close();
        }
    }
}

// End RolapConnection.java
