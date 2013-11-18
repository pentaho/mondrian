/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2013-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.resource.MondrianResource;
import mondrian.rolap.agg.*;
import mondrian.rolap.aggmatcher.JdbcSchema;
import mondrian.rolap.sql.TupleConstraint;
import mondrian.spi.*;
import mondrian.spi.impl.JndiDataSourceResolver;
import mondrian.util.ClassResolver;

import org.eigenbase.util.property.StringProperty;

import java.io.PrintWriter;
import java.lang.reflect.*;
import java.sql.*;
import java.sql.Connection;
import java.util.*;
import javax.sql.DataSource;

/**
 * Default implementation of DataServicesProvider
 */
public class DefaultDataServicesProvider implements DataServicesProvider {
    private static DataSourceResolver dataSourceResolver;
    private static JdbcSchema.Factory factory;
    private static final MondrianResource mres = MondrianResource.instance();

    public MemberReader getMemberReader(RolapCubeHierarchy hierarchy) {
        return new SqlMemberSource(hierarchy);
    }

    public SegmentLoader getSegmentLoader(SegmentCacheManager cacheMgr) {
        return new SegmentLoader(cacheMgr);
    }

    public TupleReader getTupleReader(TupleConstraint constraint) {
        return new SqlTupleReader(constraint);
    }

    public synchronized JdbcSchema.Factory getJdbcSchemaFactory() {
        if (factory != null) {
            return factory;
        }
        String className =
            MondrianProperties.instance().JdbcFactoryClass.get();
        if (className == null) {
            factory = new StdFactory();
        } else {
            try {
                Class<?> clz =
                    ClassResolver.INSTANCE.forName(className, true);
                factory = (JdbcSchema.Factory) clz.newInstance();
            } catch (ClassNotFoundException ex) {
                throw mres.BadJdbcFactoryClassName.ex(className);
            } catch (InstantiationException ex) {
                throw mres.BadJdbcFactoryInstantiation.ex(className);
            } catch (IllegalAccessException ex) {
                throw mres.BadJdbcFactoryAccess.ex(className);
            }
        }
        return factory;
    }

    protected static class StdFactory implements JdbcSchema.Factory {
        StdFactory() {
        }
        public JdbcSchema loadDatabase(DataSource dataSource) {
            return new JdbcSchema(dataSource);
        }
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
    public DataSource createDataSource(
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
                buf,
                RolapConnectionProperties.JdbcPassword.name(),
                jdbcPassword);
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
                buf,
                RolapConnectionProperties.JdbcPassword.name(),
                jdbcPassword);
            String jdbcDrivers =
                connectInfo.get(RolapConnectionProperties.JdbcDrivers.name());
            if (jdbcDrivers != null) {
                RolapUtil.loadDrivers(jdbcDrivers);
            }
            final String jdbcDriversProp =
                    MondrianProperties.instance().JdbcDrivers.get();
            RolapUtil.loadDrivers(jdbcDriversProp);

            Properties jdbcProperties =
                RolapConnection.getJdbcProperties(connectInfo);
            final Map<String, String> map = Util.toMap(jdbcProperties);
            for (Map.Entry<String, String> entry : map.entrySet()) {
                // FIXME ordering is non-deterministic
                appendKeyValue(buf, entry.getKey(), entry.getValue());
            }

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

            return RolapConnectionPool.instance()
                .getDriverManagerPoolingDataSource(
                    jdbcConnectString,
                    jdbcProperties,
                    jdbcConnectString.toLowerCase().indexOf("mysql") > -1);

        } else if (dataSourceName != null) {
            appendKeyValue(
                buf,
                RolapConnectionProperties.DataSource.name(),
                dataSourceName);
            appendKeyValue(
                buf,
                RolapConnectionProperties.JdbcUser.name(),
                jdbcUser);
            appendKeyValue(
                buf,
                RolapConnectionProperties.JdbcPassword.name(),
                jdbcPassword);

            // Data sources are fairly smart, so we assume they look after
            // their own pooling. Therefore the default is false.
            final boolean poolNeeded =
                connectInfo.get(
                    RolapConnectionProperties.PoolNeeded.name(),
                    "false").equalsIgnoreCase("true");

            // Get connection from datasource.
            DataSourceResolver dataSourceResolver = getDataSourceResolver();
            try {
                dataSource = dataSourceResolver.lookup(dataSourceName);
            } catch (Exception e) {
                throw Util.newInternal(
                    e,
                    "Error while looking up data source ("
                    + dataSourceName + ")");
            }
            if (poolNeeded) {
                dataSource =
                    RolapConnectionPool.instance()
                        .getDataSourcePoolingDataSource(
                            dataSource, dataSourceName, jdbcUser, jdbcPassword);
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
                "Connect string '" + connectInfo.toString()
                + "' must contain either '" + RolapConnectionProperties.Jdbc
                + "' or '" + RolapConnectionProperties.DataSource + "'");
        }
    }

    /**
     * Returns the instance of the {@link mondrian.spi.DataSourceResolver}
     * plugin.
     *
     * @return data source resolver
     */
    private static synchronized DataSourceResolver getDataSourceResolver() {
        if (dataSourceResolver == null) {
            final StringProperty property =
                MondrianProperties.instance().DataSourceResolverClass;
            final String className =
                property.get(
                    JndiDataSourceResolver.class.getName());
            try {
                dataSourceResolver =
                    ClassResolver.INSTANCE.instantiateSafe(className);
            } catch (ClassCastException e) {
                throw Util.newInternal(
                    e,
                    "Plugin class specified by property "
                    + property.getPath()
                    + " must implement "
                    + DataSourceResolver.class.getName());
            }
        }
        return dataSourceResolver;
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

        public java.sql.Connection getConnection() throws SQLException {
            return dataSource.getConnection(jdbcUser, jdbcPassword);
        }
    }

    /**
     * Implementation of {@link javax.sql.DataSource} which calls the good ol'
     * {@link java.sql.DriverManager}.
     *
     * <p>Overrides {@link #hashCode()} and {@link #equals(Object)} so that
     * {@link mondrian.spi.Dialect} objects can be cached more effectively.
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

        @Override
        public int hashCode() {
            int h = loginTimeout;
            h = Util.hash(h, jdbcConnectString);
            h = Util.hash(h, jdbcProperties);
            return h;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof DriverManagerDataSource) {
                DriverManagerDataSource
                    that = (DriverManagerDataSource) obj;
                return this.loginTimeout == that.loginTimeout
                    && this.jdbcConnectString.equals(that.jdbcConnectString)
                    && this.jdbcProperties.equals(that.jdbcProperties);
            }
            return false;
        }

        public Connection getConnection() throws SQLException {
            return new org.apache.commons.dbcp.DelegatingConnection(
                java.sql.DriverManager.getConnection(
                    jdbcConnectString, jdbcProperties));
        }

        public Connection getConnection(String username, String password)
            throws SQLException
        {
            if (jdbcProperties == null) {
                return java.sql.DriverManager.getConnection(
                    jdbcConnectString, username, password);
            } else {
                Properties temp = (Properties)jdbcProperties.clone();
                temp.put("user", username);
                temp.put("password", password);
                return java.sql.DriverManager.getConnection(
                    jdbcConnectString, temp);
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

        public java.util.logging.Logger getParentLogger() {
            return java.util.logging.Logger.getLogger("");
        }

        public <T> T unwrap(Class<T> iface) throws SQLException {
            throw new SQLException("not a wrapper");
        }

        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return false;
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

        public Connection getConnection(
            String username,
            String password)
            throws SQLException
        {
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

        // JDBC 4.0 support (JDK 1.6 and higher)
        public <T> T unwrap(Class<T> iface) throws SQLException {
            if (Util.JdbcVersion >= 0x0400) {
                // Do
                //              return dataSource.unwrap(iface);
                // via reflection.
                try {
                    Method method =
                        DataSource.class.getMethod("unwrap", Class.class);
                    return iface.cast(method.invoke(dataSource, iface));
                } catch (IllegalAccessException e) {
                    throw Util.newInternal(e, "While invoking unwrap");
                } catch (InvocationTargetException e) {
                    throw Util.newInternal(e, "While invoking unwrap");
                } catch (NoSuchMethodException e) {
                    throw Util.newInternal(e, "While invoking unwrap");
                }
            } else {
                if (iface.isInstance(dataSource)) {
                    return iface.cast(dataSource);
                } else {
                    return null;
                }
            }
        }

        // JDBC 4.0 support (JDK 1.6 and higher)
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            if (Util.JdbcVersion >= 0x0400) {
                // Do
                //              return dataSource.isWrapperFor(iface);
                // via reflection.
                try {
                    Method method =
                        DataSource.class.getMethod(
                            "isWrapperFor", boolean.class);
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

        // JDBC 4.1 support (JDK 1.7 and higher)
        public java.util.logging.Logger getParentLogger() {
            if (Util.JdbcVersion >= 0x0401) {
                // Do
                //              return dataSource.getParentLogger();
                // via reflection.
                try {
                    Method method =
                        DataSource.class.getMethod("getParentLogger");
                    return (java.util.logging.Logger) method.invoke(dataSource);
                } catch (IllegalAccessException e) {
                    throw Util.newInternal(e, "While invoking getParentLogger");
                } catch (InvocationTargetException e) {
                    throw Util.newInternal(e, "While invoking getParentLogger");
                } catch (NoSuchMethodException e) {
                    throw Util.newInternal(e, "While invoking getParentLogger");
                }
            } else {
                // Can't throw SQLFeatureNotSupportedException... it doesn't
                // exist before JDBC 4.1.
                throw new UnsupportedOperationException();
            }
        }
    }
}
// End DefaultDataServicesProvider.java
