/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2008 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 2 October, 2002
*/
package mondrian.rolap;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.*;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import mondrian.olap.CacheControl;
import mondrian.olap.Cell;
import mondrian.olap.ConnectionBase;
import mondrian.olap.Cube;
import mondrian.olap.DriverManager;
import mondrian.olap.MondrianProperties;
import mondrian.olap.Position;
import mondrian.olap.Query;
import mondrian.olap.QueryAxis;
import mondrian.olap.QueryCanceledException;
import mondrian.olap.QueryTimeoutException;
import mondrian.olap.ResourceLimitExceededException;
import mondrian.olap.Result;
import mondrian.olap.ResultBase;
import mondrian.olap.ResultLimitExceededException;
import mondrian.olap.Role;
import mondrian.olap.RoleImpl;
import mondrian.olap.Schema;
import mondrian.olap.SchemaReader;
import mondrian.olap.Util;
import mondrian.rolap.agg.AggregationManager;
import mondrian.rolap.sql.SqlQuery;
import mondrian.util.FilteredIterableList;
import mondrian.util.MemoryMonitor;
import mondrian.util.MemoryMonitorFactory;
import mondrian.util.Pair;

import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DataSourceConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.log4j.Logger;

/**
 * A <code>RolapConnection</code> is a connection to a Mondrian OLAP Server.
 *
 * <p>Typically, you create a connection via
 * {@link DriverManager#getConnection(String, mondrian.spi.CatalogLocator)}.
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

    // used for MDX logging, allows for a MDX Statement UID
    private static long executeCount = 0;

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
    public RolapConnection(Util.PropertyList connectInfo, DataSource dataSource) {
        this(connectInfo, null, dataSource);
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
        final String jdbcUser =
            connectInfo.get(RolapConnectionProperties.JdbcUser.name());
        final String jdbcConnectString =
            connectInfo.get(RolapConnectionProperties.Jdbc.name());
        final String strDataSource =
            connectInfo.get(RolapConnectionProperties.DataSource.name());
        StringBuilder buf = new StringBuilder();
        this.dataSource =
            createDataSource(dataSource, connectInfo, buf);
        Role role = null;
        if (schema == null) {
            // If RolapSchema.Pool.get were to call this with schema == null,
            // we would loop.
            if (dataSource == null) {
                // If there is no external data source is passed in,
                // we expect the properties Jdbc, JdbcUser, DataSource to be set,
                // as they are used to generate the schema cache key.
                final String connectionKey = jdbcConnectString +
                    getJdbcProperties(connectInfo).toString();

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
            String roleNameList =
                connectInfo.get(RolapConnectionProperties.Role.name());
            if (roleNameList != null) {
                List<String> roleNames = Util.parseCommaList(roleNameList);
                List<Role> roleList = new ArrayList<Role>();
                for (String roleName : roleNames) {
                    Role role1 = schema.lookupRole(roleName);
                    if (role1 == null) {
                        throw Util.newError("Role '" + roleName + "' not found");
                    }
                    roleList.add(role1);
                }
                switch (roleList.size()) {
                case 0:
                    // If they specify 'Role=;', the list of names will be
                    // empty, and the effect will be as if they did specify
                    // Role at all.
                    role = null;
                    break;
                case 1:
                    role = roleList.get(0);
                    break;
                default:
                    role = RoleImpl.union(roleList);
                    break;
                }
            }
        } else {
            // We are creating an internal connection. Now is a great time to
            // make sure that the JDBC credentials are valid, for this
            // connection and for external connections built on top of this.
            Connection conn = null;
            Statement statement = null;
            try {
                conn = this.dataSource.getConnection();
                SqlQuery.Dialect dialect =
                    SqlQuery.Dialect.create(conn.getMetaData());
                if (dialect.isDerby()) {
                    // Derby requires a little extra prodding to do the
                    // validation to detect an error.
                    statement = conn.createStatement();
                    statement.executeQuery("select * from bogustable");
                }
            } catch (SQLException e) {
                if (e.getMessage().equals("Table/View 'BOGUSTABLE' does not exist.")) {
                    // Ignore. This exception comes from Derby when the
                    // connection is valid. If the connection were invalid, we
                    // would receive an error such as "Schema 'BOGUSUSER' does
                    // not exist"
                } else {
                    final String message =
                        "Error while creating SQL connection: " + buf.toString();
                    throw Util.newError(e, message);
                }
            } finally {
                try {
                    if (statement != null) {
                        statement.close();
                    }
                    if (conn != null) {
                        conn.close();
                    }
                } catch (SQLException e) {
                    // ignore
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

    /**
     * Creates a JDBC data source from the JDBC credentials contained within a
     * set of mondrian connection properties.
     *
     * <p>This method is package-level so that it can be called from the
     * RolapConnectionTest unit test.
     *
     * @param dataSource Anonymous data source from user, or null
     * @param connectInfo Mondrian connection properties
     * @param buf Into which method writes a description of the JDBC credentials
     * @return Data source
     */
    static DataSource createDataSource(
        DataSource dataSource,
        Util.PropertyList connectInfo,
        StringBuilder buf)
    {
        assert buf != null;
        final String jdbcConnectString =
            connectInfo.get(RolapConnectionProperties.Jdbc.name());
        final String jdbcUser =
            connectInfo.get(RolapConnectionProperties.JdbcUser.name());
        final String jdbcPassword =
            connectInfo.get(RolapConnectionProperties.JdbcPassword.name());
        final String dataSourceName =
            connectInfo.get(RolapConnectionProperties.DataSource.name());

        if (dataSource != null) {
            appendKeyValue(buf, "Anonymous data source", dataSource);
            appendKeyValue(
                buf, RolapConnectionProperties.JdbcUser.name(), jdbcUser);
            appendKeyValue(
                buf, RolapConnectionProperties.JdbcPassword.name(), jdbcPassword);
            if (jdbcUser != null || jdbcPassword != null) {
                dataSource =
                    new UserPasswordDataSource(
                        dataSource, jdbcUser, jdbcPassword);
            }
            return dataSource;

        } else if (jdbcConnectString != null) {
            // Get connection through own pooling datasource
            appendKeyValue(
                buf, RolapConnectionProperties.Jdbc.name(), jdbcConnectString);
            appendKeyValue(
                buf, RolapConnectionProperties.JdbcUser.name(), jdbcUser);
            appendKeyValue(
                buf, RolapConnectionProperties.JdbcPassword.name(), jdbcPassword);
            String jdbcDrivers =
                connectInfo.get(RolapConnectionProperties.JdbcDrivers.name());
            if (jdbcDrivers != null) {
                RolapUtil.loadDrivers(jdbcDrivers);
            }
            final String jdbcDriversProp =
                    MondrianProperties.instance().JdbcDrivers.get();
            RolapUtil.loadDrivers(jdbcDriversProp);

            Properties jdbcProperties = getJdbcProperties(connectInfo);
            for (Map.Entry<Object, Object> entry : jdbcProperties.entrySet()) {
                // FIXME ordering is non-deterministic
                appendKeyValue(buf, (String) entry.getKey(), entry.getValue());
            }
            String propertyString = jdbcProperties.toString();
            if (jdbcUser != null) {
                jdbcProperties.put("user", jdbcUser);
            }
            if (jdbcPassword != null) {
                jdbcProperties.put("password", jdbcPassword);
            }

            // JDBC connections are dumb beasts, so we assume they're not
            // pooled. Therefore the default is true.
            final boolean poolNeeded =
                connectInfo.get(
                    RolapConnectionProperties.PoolNeeded.name(),
                    "true").equalsIgnoreCase("true");

            if (!poolNeeded) {
                // Connection is already pooled; don't pool it again.
                return new DriverManagerDataSource(
                    jdbcConnectString,
                    jdbcProperties);
            }

            if (jdbcConnectString.toLowerCase().indexOf("mysql") > -1) {
                // mysql driver needs this autoReconnect parameter
                jdbcProperties.setProperty("autoReconnect", "true");
            }
            // use the DriverManagerConnectionFactory to create connections
            ConnectionFactory connectionFactory =
                new DriverManagerConnectionFactory(
                    jdbcConnectString,
                    jdbcProperties);
            try {
                return RolapConnectionPool.instance().getPoolingDataSource(
                    jdbcConnectString + propertyString,
                    connectionFactory);
            } catch (Throwable e) {
                throw Util.newInternal(
                    e,
                    "Error while creating connection pool (with URI " +
                        jdbcConnectString + ")");
            }

        } else if (dataSourceName != null) {
            appendKeyValue(
                buf, RolapConnectionProperties.DataSource.name(), dataSourceName);
            appendKeyValue(
                buf, RolapConnectionProperties.JdbcUser.name(), jdbcUser);
            appendKeyValue(
                buf, RolapConnectionProperties.JdbcPassword.name(), jdbcPassword);

            // Data sources are fairly smart, so we assume they look after
            // their own pooling. Therefore the default is false.
            final boolean poolNeeded =
                connectInfo.get(
                    RolapConnectionProperties.PoolNeeded.name(),
                    "false").equalsIgnoreCase("true");

            // Get connection from datasource.
            try {
                dataSource =
                    (DataSource) new InitialContext().lookup(dataSourceName);
            } catch (NamingException e) {
                throw Util.newInternal(
                    e,
                    "Error while looking up data source (" +
                        dataSourceName + ")");
            }
            if (poolNeeded) {
                ConnectionFactory connectionFactory;
                if (jdbcUser != null || jdbcPassword != null) {
                    connectionFactory =
                        new DataSourceConnectionFactory(
                            dataSource, jdbcUser, jdbcPassword);
                } else {
                    connectionFactory =
                        new DataSourceConnectionFactory(dataSource);
                }
                try {
                    dataSource =
                        RolapConnectionPool.instance().getPoolingDataSource(
                            dataSourceName,
                            connectionFactory);
                } catch (Exception e) {
                    throw Util.newInternal(
                        e,
                        "Error while creating connection pool (with URI " +
                            dataSourceName + ")");
                }
            } else {
                if (jdbcUser != null || jdbcPassword != null) {
                    dataSource =
                        new UserPasswordDataSource(
                            dataSource, jdbcUser, jdbcPassword);
                }
            }
            return dataSource;
        } else {
            throw Util.newInternal(
                "Connect string '" + connectInfo.toString() +
                    "' must contain either '" + RolapConnectionProperties.Jdbc +
                    "' or '" + RolapConnectionProperties.DataSource + "'");
        }
    }

    /**
     * Appends "key=value" to a buffer, if value is not null.
     *
     * @param buf Buffer
     * @param key Key
     * @param value Value
     */
    private static void appendKeyValue(
        StringBuilder buf,
        String key,
        Object value)
    {
        if (value != null) {
            if (buf.length() > 0) {
                buf.append("; ");
            }
            buf.append(key).append('=').append(value);
        }
    }

    /**
     * Creates a {@link Properties} object containing all of the JDBC
     * connection properties present in the
     * {@link mondrian.olap.Util.PropertyList connectInfo}.
     *
     * @param connectInfo Connection properties
     * @return The JDBC connection properties.
     */
    private static Properties getJdbcProperties(Util.PropertyList connectInfo) {
        Properties jdbcProperties = new Properties();
        for (Pair<String,String> entry : connectInfo) {
            if (entry.left.startsWith(
                RolapConnectionProperties.JdbcPropertyPrefix))
            {
                jdbcProperties.put(
                    entry.left.substring(
                        RolapConnectionProperties.JdbcPropertyPrefix.length()),
                    entry.right);
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
        MemoryMonitor mm = MemoryMonitorFactory.getMemoryMonitor();
        long currId = -1;
        try {
            mm.addListener(listener);
            // Check to see if we must punt
            query.checkCancelOrTimeout();

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(Util.unparse(query));
            }

            if (RolapUtil.MDX_LOGGER.isDebugEnabled()) {
                currId = executeCount++;
                RolapUtil.MDX_LOGGER.debug(currId + ": " + Util.unparse(query));
            }

            query.setQueryStartTime();
            Result result = new RolapResult(query, true);
            for (int i = 0; i < query.axes.length; i++) {
                QueryAxis axis = query.axes[i];
                if (axis.isNonEmpty()) {
                    result = new NonEmptyResult(result, query, i);
                }
            }
            /* It will not work with HighCardinality.
            if (LOGGER.isDebugEnabled()) {
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                result.print(pw);
                pw.flush();
                LOGGER.debug(sw.toString());
            }
            */
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
            if (RolapUtil.MDX_LOGGER.isDebugEnabled()) {
                RolapUtil.MDX_LOGGER.debug(currId + ": exec: " +
                    (System.currentTimeMillis() - query.getQueryStartTime()) +
                    " ms");
            }
        }
    }

    public void setRole(Role role) {
        assert role != null;

        this.role = role;
        this.schemaReader = new RolapSchemaReader(role, schema) {
            public Cube getCube() {
                throw new UnsupportedOperationException();
            }
        };
    }

    public Role getRole() {
        Util.assertPostcondition(role != null, "role != null");

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

        public DriverManagerDataSource(
            String jdbcConnectString,
            Properties properties)
        {
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
    static class NonEmptyResult extends ResultBase {

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

            final List<Position> positionsList;
            try {
                if (positions.get(0).get(0).getDimension().isHighCardinality()) {
                    positionsList =
                        new FilteredIterableList<Position>(
                            positions,
                            new FilteredIterableList.Filter<Position>() {
                                public boolean accept(final Position p) {
                                    return p.get(0) != null;
                                }
                            }
                    );
                } else {
                    positionsList = new ArrayList<Position>();
                    int i = -1;
                    for (Position position : positions) {
                        ++i;
                        if (! isEmpty(i, axis)) {
                            map.put(positionsList.size(), i);
                            positionsList.add(position);
                        }
                    }
                }
                this.axes[axis] = new RolapAxis.PositionList(positionsList);
            } catch (IndexOutOfBoundsException ioobe) {
                // No elements.
                this.axes[axis] =
                    new RolapAxis.PositionList(
                            new ArrayList<Position>());
            }
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
                for (Position position : positions) {
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
            try {
                System.arraycopy(externalPos, 0, this.pos, 0, externalPos.length);
                int offset = externalPos[axis];
                int mappedOffset = mapOffsetToUnderlying(offset);
                this.pos[axis] = mappedOffset;
                return underlying.getCell(this.pos);
            } catch (NullPointerException npe) {
                System.out.println("RolapConnection:619 please, review for "
                        + "high cardinality...");
                return underlying.getCell(externalPos);
            }
        }

        private int mapOffsetToUnderlying(int offset) {
            return map.get(offset);
        }

        public void close() {
            underlying.close();
        }
    }

    /**
     * Data source that delegates all methods to an underlying data source.
     */
    private static abstract class DelegatingDataSource implements DataSource {
        protected final DataSource dataSource;

        public DelegatingDataSource(DataSource dataSource) {
            this.dataSource = dataSource;
        }

        public Connection getConnection() throws SQLException {
            return dataSource.getConnection();
        }

        public Connection getConnection(String username, String password) throws SQLException {
            return dataSource.getConnection(username, password);
        }

        public PrintWriter getLogWriter() throws SQLException {
            return dataSource.getLogWriter();
        }

        public void setLogWriter(PrintWriter out) throws SQLException {
            dataSource.setLogWriter(out);
        }

        public void setLoginTimeout(int seconds) throws SQLException {
            dataSource.setLoginTimeout(seconds);
        }

        public int getLoginTimeout() throws SQLException {
            return dataSource.getLoginTimeout();
        }

        public <T> T unwrap(Class<T> iface) throws SQLException {
            if (Util.JdbcVersion >= 4) {
                // Do
                //              return dataSource.unwrap(iface);
                // via reflection.
                try {
                    Method method =
                        DataSource.class.getMethod("unwrap", Class.class);
                    return iface.cast(method.invoke(dataSource, iface));
                } catch (IllegalAccessException e) {
                    throw Util.newInternal(e, "While invokin unwrap");
                } catch (InvocationTargetException e) {
                    throw Util.newInternal(e, "While invokin unwrap");
                } catch (NoSuchMethodException e) {
                    throw Util.newInternal(e, "While invokin unwrap");
                }
            } else {
                if (iface.isInstance(dataSource)) {
                    return iface.cast(dataSource);
                } else {
                    return null;
                }
            }
        }

        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            if (Util.JdbcVersion >= 4) {
                // Do
                //              return dataSource.isWrapperFor(iface);
                // via reflection.
                try {
                    Method method =
                        DataSource.class.getMethod("isWrapperFor", boolean.class);
                    return (Boolean) method.invoke(dataSource, iface);
                } catch (IllegalAccessException e) {
                    throw Util.newInternal(e, "While invoking isWrapperFor");
                } catch (InvocationTargetException e) {
                    throw Util.newInternal(e, "While invoking isWrapperFor");
                } catch (NoSuchMethodException e) {
                    throw Util.newInternal(e, "While invoking isWrapperFor");
                }
            } else {
                return iface.isInstance(dataSource);
            }
        }
    }

    /**
     * Data source that gets connections from an underlying data source but
     * with different user name and password.
     */
    private static class UserPasswordDataSource extends DelegatingDataSource {
        private final String jdbcUser;
        private final String jdbcPassword;

        /**
         * Creates a UserPasswordDataSource
         *
         * @param dataSource Underlying data source
         * @param jdbcUser User name
         * @param jdbcPassword Password
         */
        public UserPasswordDataSource(
            DataSource dataSource,
            String jdbcUser,
            String jdbcPassword)
        {
            super(dataSource);
            this.jdbcUser = jdbcUser;
            this.jdbcPassword = jdbcPassword;
        }

        public Connection getConnection() throws SQLException {
            return dataSource.getConnection(jdbcUser, jdbcPassword);
        }
    }
}

// End RolapConnection.java
