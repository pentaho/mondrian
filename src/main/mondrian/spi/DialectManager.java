/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.spi;

import mondrian.olap.Util;
import mondrian.spi.impl.JdbcDialectFactory;
import mondrian.spi.impl.JdbcDialectImpl;
import mondrian.util.ClassResolver;
import mondrian.util.ServiceDiscovery;

import java.lang.reflect.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import javax.sql.DataSource;

/**
 * Manages {@link mondrian.spi.Dialect} and {@link mondrian.spi.DialectFactory}
 * objects.
 *
 * @author jhyde
 * @since Jan 13, 2009
 */
public abstract class DialectManager {
    /**
     * The singleton instance of the implementation class.
     */
    private static final DialectManagerImpl IMPL = new DialectManagerImpl();

    /**
     * DialectManager is not instantiable.
     */
    private DialectManager() {
        throw new IllegalArgumentException();
    }

    /**
     * Registers a DialectFactory.
     *
     * @param factory Dialect factory
     */
    public static void register(DialectFactory factory) {
        IMPL.register(factory);
    }

    /**
     * Registers a Dialect class.
     *
     * @param dialectClass Dialect class
     */
    public static void register(Class<? extends Dialect> dialectClass) {
        IMPL.register(dialectClass);
    }

    /**
     * Creates a Dialect from a JDBC connection.
     *
     * <p>If the dialect cannot handle this connection, throws. Never returns
     * null.
     *
     * @param dataSource Data source
     *
     * @param connection JDBC connection
     *
     * @return dialect for this connection
     *
     * @throws RuntimeException if underlying systems give an error,
     * or if cannot create dialect
     */
    public static Dialect createDialect(
        DataSource dataSource,
        Connection connection)
    {
        return createDialect(dataSource, connection, null);
    }

    /**
     * Creates a Dialect from a JDBC connection, optionally specifying
     * the name of the dialect class.
     *
     * <p>If the dialect cannot handle this connection, throws. Never returns
     * null.
     *
     * @param dataSource Data source
     *
     * @param connection JDBC connection
     *
     * @param dialectClassName Name of class that implements {@link Dialect},
     * or null
     *
     * @return dialect for this connection
     *
     * @throws RuntimeException if underlying systems give an error,
     * or if cannot create dialect
     */
    public static Dialect createDialect(
        DataSource dataSource,
        Connection connection,
        String dialectClassName)
    {
        return IMPL.createDialect(dataSource, connection, dialectClassName);
    }

    /**
     * Creates a factory that calls a public constructor of a dialect class.
     *
     * @param dialectClass Dialect class
     * @return Factory, or null if the class has no suitable constructor.
     */
    static DialectFactory createFactoryForDialect(
        Class<? extends Dialect> dialectClass)
    {
        // If there is a public, static member called FACTORY,
        // use it.
        for (Field field : dialectClass.getFields()) {
            if (Modifier.isPublic(field.getModifiers())
                && Modifier.isStatic(field.getModifiers())
                && field.getName().equals("FACTORY")
                && DialectFactory.class.isAssignableFrom(field.getType()))
            {
                try {
                    final DialectFactory
                        factory = (DialectFactory) field.get(null);
                    if (factory != null) {
                        return factory;
                    }
                } catch (IllegalAccessException e) {
                    throw Util.newError(
                        e,
                        "Error while accessing field " + field);
                }
            }
        }
        // Otherwise, create a factory that calls the
        // 'public <init>(Connection)' constructor.
        try {
            final Constructor<? extends Dialect> constructor =
                dialectClass.getConstructor(Connection.class);
            if (Modifier.isPublic(constructor.getModifiers())) {
                return new ConstructorDialectFactory(constructor);
            }
        } catch (NoSuchMethodException e) {
            // ignore
        }

        // No suitable constructor or factory.
        return null;
    }

    /**
     * Implementation class for {@link mondrian.spi.DialectManager}.
     *
     * <p><code>DialectManagerImpl</code> has a non-static method for each
     * public static method in <code>DialectManager</code>.
     */
    private static class DialectManagerImpl {
        private final ChainDialectFactory registeredFactory;
        private final DialectFactory factory;

        /**
         * Creates a DialectManagerImpl.
         *
         * <p>Loads all dialects that can be found on the classpath according to
         * the JAR service provider specification. (See
         * {@link mondrian.util.ServiceDiscovery} for more details.)
         */
        DialectManagerImpl() {
            final List<DialectFactory>
                list = new ArrayList<DialectFactory>();
            final List<Class<Dialect>> dialectClasses =
                ServiceDiscovery.forClass(Dialect.class).getImplementor();
            for (Class<Dialect> dialectClass : dialectClasses) {
                DialectFactory factory =
                    createFactoryForDialect(dialectClass);
                if (factory != null) {
                    list.add(factory);
                }
            }
            registeredFactory = new ChainDialectFactory(list);

            final DialectFactory fallbackFactory =
                new DialectFactory() {
                    public Dialect createDialect(
                        DataSource dataSource,
                        Connection connection)
                    {
                        // If connection is null, create a temporary connection
                        // and recursively call this method.
                        if (connection == null) {
                            return JdbcDialectFactory.createDialectHelper(
                                this, dataSource);
                        }
                        try {
                            return new JdbcDialectImpl(connection);
                        } catch (SQLException e) {
                            throw Util.newError(
                                e,
                                "Error while creating a generic dialect for"
                                + " JDBC connection" + connection);
                        }
                    }
                };
            // The system dialect factory first walks the chain of registered
            // dialect factories (registered implicitly based on service
            // discovery, or explicitly by calling register), then uses the JDBC
            // dialect factory as a fallback.
            //
            // It caches based on data source.
            factory =
                new CachingDialectFactory(
                    new ChainDialectFactory(
                        Arrays.asList(
                            registeredFactory,
                            fallbackFactory)));
        }

        /**
         * Implements {@link DialectManager#register(DialectFactory)}.
         *
         * @param factory Dialect factory
         */
        synchronized void register(DialectFactory factory) {
            if (factory == null) {
                throw new IllegalArgumentException();
            }
            registeredFactory.dialectFactoryList.add(factory);
        }

        /**
         * Implements {@link DialectManager#register(Class)}.
         *
         * @param dialectClass Dialect class
         */
        synchronized void register(Class<? extends Dialect> dialectClass) {
            if (dialectClass == null) {
                throw new IllegalArgumentException();
            }
            register(createFactoryForDialect(dialectClass));
        }

        /**
         * Implements {@link DialectManager#createDialect(javax.sql.DataSource,java.sql.Connection)}.
         *
         * <p>The method synchronizes on a singleton class, so prevents two
         * threads from accessing any dialect factory simultaneously.
         *
         * @param dataSource Data source
         * @param connection Connection
         * @return Dialect, never null
         */
        synchronized Dialect createDialect(
            DataSource dataSource,
            Connection connection,
            String dialectClassName)
        {
            if (dataSource == null && connection == null) {
                throw new IllegalArgumentException();
            }
            final DialectFactory factory;
            if (dialectClassName != null) {
                // Instantiate explicit dialect class.
                try {
                    Class<? extends Dialect> dialectClass =
                        ClassResolver.INSTANCE.forName(dialectClassName, true)
                            .asSubclass(Dialect.class);
                    factory = createFactoryForDialect(dialectClass);
                } catch (ClassCastException e) {
                    throw new RuntimeException(
                        "Dialect class " + dialectClassName
                        + " does not implement interface " + Dialect.class);
                } catch (Exception e) {
                    throw new RuntimeException(
                        "Cannot instantiate dialect class '"
                            + dialectClassName + "'",
                        e);
                }
            } else {
                // Use factory of dialects registered in services file.
                factory = this.factory;
            }
            final Dialect dialect =
                factory.createDialect(dataSource, connection);
            if (dialect == null) {
                throw Util.newError(
                    "Cannot create dialect for JDBC connection" + connection);
            }
            return dialect;
        }
    }

    /**
     * Implementation of {@link DialectFactory} that tries to
     * create a Dialect using a succession of underlying factories.
     */
    static class ChainDialectFactory implements DialectFactory {
        private final List<DialectFactory> dialectFactoryList;

        /**
         * Creates a ChainDialectFactory.
         *
         * @param dialectFactoryList List of underlying factories
         */
        ChainDialectFactory(List<DialectFactory> dialectFactoryList) {
            this.dialectFactoryList = dialectFactoryList;
        }

        public Dialect createDialect(
            DataSource dataSource,
            Connection connection)
        {
            // Make sure that there is a connection.
            // If connection is null, create a temporary connection and
            // recursively call this method.
            // It's more efficient to create the connection here than to
            // require each chained factory to create a connection.
            if (connection == null) {
                return JdbcDialectFactory.createDialectHelper(this, dataSource);
            }

            for (DialectFactory factory : dialectFactoryList) {
                // REVIEW: If createDialect throws, should we carry on?
                final Dialect dialect =
                    factory.createDialect(
                        dataSource,
                        connection);
                if (dialect != null) {
                    return dialect;
                }
            }
            return null;
        }
    }

    /**
     * Implementation of {@link DialectFactory} that calls
     * a class's {@code public &lt;init&gt;(Connection connection)} constructor.
     */
    static class ConstructorDialectFactory implements DialectFactory {
        private final Constructor<? extends Dialect> constructor;

        /**
         * Creates a ConstructorDialectFactory.
         *
         * @param constructor Constructor
         */
        ConstructorDialectFactory(
            Constructor<? extends Dialect> constructor)
        {
            assert constructor != null;
            assert constructor.getParameterTypes().length == 1;
            assert constructor.getParameterTypes()[0]
                == java.sql.Connection.class;
            this.constructor = constructor;
        }

        public Dialect createDialect(
            DataSource dataSource,
            Connection connection)
        {
            // If connection is null, create a temporary connection
            // and recursively call this method.
            if (connection == null) {
                return JdbcDialectFactory.createDialectHelper(
                    this, dataSource);
            }

            // Connection is not null. Invoke the constructor.
            try {
                return constructor.newInstance(connection);
            } catch (InstantiationException e) {
                throw Util.newError(
                    e,
                    "Error while instantiating dialect of class "
                    + constructor.getClass());
            } catch (IllegalAccessException e) {
                throw Util.newError(
                    e,
                    "Error while instantiating dialect of class "
                    + constructor.getClass());
            } catch (InvocationTargetException e) {
                throw Util.newError(
                    e,
                    "Error while instantiating dialect of class "
                    + constructor.getClass());
            }
        }
    }

    /**
     * Implementation of {@link mondrian.spi.DialectFactory} that caches
     * dialects based on data source.
     *
     * @see mondrian.spi.Dialect#allowsDialectSharing()
     */
    static class CachingDialectFactory implements DialectFactory {
        private final DialectFactory factory;
        private final Map<DataSource, Dialect> dataSourceDialectMap =
            new WeakHashMap<DataSource, Dialect>();

        /**
         * Creates a CachingDialectFactory.
         *
         * @param factory Underlying factory
         */
        CachingDialectFactory(DialectFactory factory) {
            this.factory = factory;
        }

        public Dialect createDialect(
            DataSource dataSource,
            Connection connection)
        {
            if (dataSource != null) {
                Dialect dialect = dataSourceDialectMap.get(dataSource);
                if (dialect != null) {
                    return dialect;
                }
            }

            // No cached dialect. Get a dialect from the underlying factory.
            final Dialect dialect =
                factory.createDialect(dataSource, connection);

            // Put the dialect into the cache if it is sharable.
            if (dialect != null
                && dataSource != null
                && dialect.allowsDialectSharing())
            {
                dataSourceDialectMap.put(dataSource, dialect);
            }
            return dialect;
        }
    }
}

// End DialectManager.java
