/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2013 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.calc.*;
import mondrian.olap.*;
import mondrian.parser.MdxParserValidator;
import mondrian.resource.MondrianResource;
import mondrian.server.*;
import mondrian.spi.*;
import mondrian.util.*;

import org.apache.log4j.Logger;

import org.olap4j.Scenario;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;

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
 */
public class RolapConnection extends ConnectionBase {
    private static final Logger LOGGER =
        Logger.getLogger(RolapConnection.class);
    private static final AtomicInteger ID_GENERATOR = new AtomicInteger();

    private final MondrianServer server;

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
    private Locale locale = Locale.getDefault();
    private Scenario scenario;
    private boolean closed = false;

    private final int id;
    private final Statement internalStatement;
    final Dialect dialect; // set only for internal connections

    /**
     * Creates a connection.
     *
     * @param server Server instance this connection belongs to
     * @param connectInfo Connection properties; keywords are described in
     *   {@link RolapConnectionProperties}.
     * @param dataSource JDBC data source
     */
    public RolapConnection(
        MondrianServer server,
        Util.PropertyList connectInfo,
        DataSource dataSource)
    {
        this(server, connectInfo, null, dataSource);
    }

    /**
     * Creates a RolapConnection.
     *
     * <p>Only {@link mondrian.rolap.RolapSchemaPool#get} calls this with
     * schema != null (to create a schema's internal connection).
     * Other uses retrieve a schema from the cache based upon
     * the <code>Catalog</code> property.
     *
     * @param server Server instance this connection belongs to
     * @param connectInfo Connection properties; keywords are described in
     *   {@link RolapConnectionProperties}.
     * @param schema Schema for the connection. Must be null unless this is to
     *   be an internal connection.
     * @param dataSource If not null an external DataSource to be used
     *        by Mondrian
     */
    RolapConnection(
        MondrianServer server,
        Util.PropertyList connectInfo,
        RolapSchema schema,
        DataSource dataSource)
    {
        super();
        assert server != null;
        this.server = server;
        this.id = ID_GENERATOR.getAndIncrement();

        assert connectInfo != null;
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
        DataServicesProvider dataServicesProvider =
            DataServicesLocator.getDataServicesProvider(
                connectInfo.get(
                    RolapConnectionProperties.DataServicesProvider.name()));
        this.dataSource =
            dataServicesProvider.createDataSource(dataSource, connectInfo, buf);
        RolapSchema.RoleFactory roleFactory = null;

        // Register this connection before we register its internal statement.
        server.addConnection(this);

        if (schema == null) {
            this.dialect = null;

            // If RolapSchema.Pool.get were to call this with schema == null,
            // we would loop.
            Statement bootstrapStatement = createInternalStatement(false);
            final Locus locus =
                new Locus(
                    new Execution(bootstrapStatement, 0),
                    null,
                    "Initializing connection");
            Locus.push(locus);
            try {
                if (dataSource == null) {
                    // If there is no external data source is passed in, we
                    // expect the properties Jdbc, JdbcUser, DataSource to be
                    // set, as they are used to generate the schema cache key.
                    final String connectionKey =
                        jdbcConnectString
                        + getJdbcProperties(connectInfo).toString();

                    schema = RolapSchemaPool.instance().get(
                        catalogUrl,
                        connectionKey,
                        jdbcUser,
                        strDataSource,
                        connectInfo);
                } else {
                    schema = RolapSchemaPool.instance().get(
                        catalogUrl,
                        dataSource,
                        connectInfo);
                }
            } finally {
                Locus.pop(locus);
                bootstrapStatement.close();
            }
            internalStatement =
                schema.getInternalConnection().getInternalStatement();
            String roleNameList =
                connectInfo.get(RolapConnectionProperties.Role.name());
            if (roleNameList != null) {
                List<String> roleNames = Util.parseCommaList(roleNameList);
                List<RolapSchema.RoleFactory> roleList =
                    new ArrayList<RolapSchema.RoleFactory>();
                for (String roleName : roleNames) {
                    roleList.add(getRoleFactory(server, schema, roleName));
                }
                switch (roleList.size()) {
                case 0:
                    // If they specify 'Role=;', the list of names will be
                    // empty, and the effect will be as if they did specify
                    // Role at all.
                    roleFactory = null;
                    break;
                case 1:
                    roleFactory = roleList.get(0);
                    break;
                default:
                    roleFactory = new RolapSchema.UnionRoleFactory(roleList);
                    break;
                }
            }
        } else {
            this.internalStatement = createInternalStatement(true);

            // We are creating an internal connection. Now is a great time to
            // make sure that the JDBC credentials are valid, for this
            // connection and for external connections built on top of this.
            Connection conn = null;
            java.sql.Statement statement = null;
            Dialect dialect = null;
            try {
                conn = this.dataSource.getConnection();
                final String dialectClassName =
                    connectInfo.get(RolapConnectionProperties.Dialect.name());
                dialect = DialectManager.createDialect(
                    this.dataSource, conn, dialectClassName);
                if (dialect.getDatabaseProduct()
                    == Dialect.DatabaseProduct.DERBY)
                {
                    // Derby requires a little extra prodding to do the
                    // validation to detect an error.
                    statement = conn.createStatement();
                    try {
                        statement.executeQuery("select * from bogustable");
                    } catch (SQLException e) {
                        if (e.getMessage().equals(
                                "Table/View 'BOGUSTABLE' does not exist."))
                        {
                            // Ignore. This exception comes from Derby when the
                            // connection is valid. If the connection were
                            // invalid, we would receive an error such as
                            // "Schema 'BOGUSUSER' does not exist"
                        } else {
                            throw e;
                        }
                    }
                }
            } catch (SQLException e) {
                throw Util.newError(
                    e,
                    "Error while creating SQL connection: " + buf);
            } finally {
                this.dialect = dialect;
                Util.close(null, statement, conn);
            }
        }

        if (roleFactory == null) {
            roleFactory = schema.getDefaultRole();
        }

        // Set the locale.
        String localeString =
            connectInfo.get(RolapConnectionProperties.Locale.name());
        if (localeString != null) {
            this.locale = Util.parseLocale(localeString);
            assert locale != null;
        }

        this.schema = schema;
        final Map<String, Object> context = new HashMap<String, Object>();
        for (Pair<String, String> pair : connectInfo) {
            if (pair.left.startsWith(
                    RolapConnectionProperties.RolePropertyPrefix))
            {
                context.put(
                    pair.left.substring(
                        RolapConnectionProperties.RolePropertyPrefix.length()),
                    pair.right);
            }
        }
        Role role = roleFactory.create(context);
        setRole(role);
    }

    private RolapSchema.RoleFactory getRoleFactory(
        MondrianServer server,
        RolapSchema schema,
        String roleName)
    {
        // First look in lock-box
        final LockBox.Entry entry =
            server.getLockBox().get(roleName);
        if (entry != null) {
            try {
                final Object value = entry.getValue();
                if (value instanceof RolapSchema.RoleFactory) {
                    return (RolapSchema.RoleFactory) value;
                } else {
                    return new RolapSchema.ConstantRoleFactory((Role) value);
                }
            } catch (ClassCastException e) {
                // ignore lock box
            }
        }

        // Now look up role factory in schema
        RolapSchema.RoleFactory factory =
            schema.mapNameToRole.get(roleName);
        if (factory != null) {
            return factory;
        }
        throw Util.newError("Role '" + roleName + "' not found");
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            super.finalize();
            close();
        } catch (Throwable t) {
            LOGGER.info(
                MondrianResource.instance()
                    .FinalizerErrorRolapConnection.baseMessage,
                t);
        }
    }

    /**
     * Returns the identifier of this connection. Unique within the lifetime of
     * this JVM.
     *
     * @return Identifier of this connection
     */
    public int getId() {
        return id;
    }

    protected Logger getLogger() {
        return LOGGER;
    }

    /**
     * Creates a {@link Properties} object containing all of the JDBC
     * connection properties present in the
     * {@link mondrian.olap.Util.PropertyList connectInfo}.
     *
     * @param connectInfo Connection properties
     * @return The JDBC connection properties.
     */
    static Properties getJdbcProperties(Util.PropertyList connectInfo) {
        Properties jdbcProperties = new Properties();
        for (Pair<String, String> entry : connectInfo) {
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
        if (!closed) {
            closed = true;
            server.removeConnection(this);
        }
    }

    public RolapSchema getSchema() {
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
        if (locale == null) {
            throw new IllegalArgumentException("locale must not be null");
        }
        this.locale = locale;
    }

    public SchemaReader getSchemaReader() {
        return schemaReader;
    }

    public Object getProperty(String name) {
        // Mask out the values of certain properties.
        if (name.equals(RolapConnectionProperties.JdbcPassword.name())
            || name.equals(RolapConnectionProperties.CatalogContent.name()))
        {
            return "";
        }
        return connectInfo.get(name);
    }

    public CacheControl getCacheControl(PrintWriter pw) {
        return getServer().getAggregationManager().getCacheControl(this, pw);
    }

    /**
     * Executes a Query.
     *
     * @param query Query parse tree
     *
     * @throws ResourceLimitExceededException if some resource limit specified
     *     in the property file was exceeded
     * @throws QueryCanceledException if query was canceled during execution
     * @throws QueryTimeoutException if query exceeded timeout specified in
     *     the property file
     *
     * @deprecated Use {@link #execute(mondrian.server.Execution)}; this method
     *     will be removed in mondrian-4.0
     */
    public Result execute(Query query) {
        final Statement statement = query.getStatement();
        Execution execution =
            new Execution(statement, statement.getQueryTimeoutMillis());
        return execute(execution);
    }

    /**
     * Executes a statement.
     *
     * @param execution Execution context (includes statement, query)
     *
     * @throws ResourceLimitExceededException if some resource limit specified
     *     in the property file was exceeded
     * @throws QueryCanceledException if query was canceled during execution
     * @throws QueryTimeoutException if query exceeded timeout specified in
     *     the property file
     */
    public Result execute(final Execution execution) {
        execution.copyMDC();
        return
            server.getResultShepherd()
                .shepherdExecution(
                    execution,
                    new Callable<Result>() {
                        public Result call() throws Exception {
                            return executeInternal(execution);
                        }
                    });
    }

    private Result executeInternal(final Execution execution) {
        execution.setContextMap();
        final Statement statement = execution.getMondrianStatement();
        // Cleanup any previous executions still running
        synchronized (statement) {
            final Execution previousExecution =
                statement.getCurrentExecution();
            if (previousExecution != null) {
                statement.end(previousExecution);
            }
        }
        final Query query = statement.getQuery();
        final MemoryMonitor.Listener listener = new MemoryMonitor.Listener() {
            public void memoryUsageNotification(long used, long max) {
                execution.setOutOfMemory(
                    "OutOfMemory used="
                    + used
                    + ", max="
                    + max
                    + " for connection: "
                    + getConnectString());
            }
        };
        MemoryMonitor mm = MemoryMonitorFactory.getMemoryMonitor();
        final long currId = execution.getId();
        try {
            mm.addListener(listener);
            // Check to see if we must punt
            execution.checkCancelOrTimeout();

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(Util.unparse(query));
            }

            if (RolapUtil.MDX_LOGGER.isDebugEnabled()) {
                RolapUtil.MDX_LOGGER.debug(currId + ": " + Util.unparse(query));
            }

            final Locus locus = new Locus(execution, null, "Loading cells");
            Locus.push(locus);
            Result result;
            final RolapCube cube = (RolapCube) query.getCube();
            try {
                statement.start(execution);
                for (RolapStar star : cube.getStars()) {
                    star.clearCachedAggregations(true);
                }
                result = new RolapResult(execution, true);
                int i = 0;
                for (QueryAxis axis : query.getAxes()) {
                    if (axis.isNonEmpty()) {
                        result = new NonEmptyResult(result, execution, i);
                    }
                    ++i;
                }
            } finally {
                Locus.pop(locus);
                for (RolapStar star : cube.getStars()) {
                    star.clearCachedAggregations(true);
                }
            }
            statement.end(execution);
            return result;
        } catch (ResultLimitExceededException e) {
            // query has been punted
            throw e;
        } catch (Exception e) {
            try {
                statement.end(execution);
            } catch (Exception e1) {
                // We can safely ignore that cleanup exception.
                // If an error is encountered here, it means that
                // one was already encountered at statement.start()
                // above and the exception we will throw after the
                // cleanup is the same as the original one.
            }
            String queryString;
            try {
                queryString = Util.unparse(query);
            } catch (Exception e1) {
                queryString = "?";
            }
            throw Util.newError(
                e,
                "Error while executing query [" + queryString + "]");
        } finally {
            mm.removeListener(listener);
            if (RolapUtil.MDX_LOGGER.isDebugEnabled()) {
                final long elapsed = execution.getElapsedMillis();
                RolapUtil.MDX_LOGGER.debug(
                    currId + ": exec: " + elapsed + " ms");
            }
        }
    }

    public void setRole(Role role) {
        assert role != null;

        this.role = role;
        this.schemaReader = new RolapSchemaReader(role, schema);
    }

    public Role getRole() {
        Util.assertPostcondition(role != null, "role != null");

        return role;
    }

    public void setScenario(Scenario scenario) {
        this.scenario = scenario;
    }

    public Scenario getScenario() {
        return scenario;
    }

    /**
     * Returns the server (mondrian instance) that this connection belongs to.
     * Usually there is only one server instance in a given JVM.
     *
     * @return Server instance; never null
     */
    public MondrianServer getServer() {
        return server;
    }

    public QueryPart parseStatement(String query) {
        Statement statement = createInternalStatement(false);
        final Locus locus =
            new Locus(
                new Execution(statement, 0),
                "Parse/validate MDX statement",
                null);
        Locus.push(locus);
        try {
            QueryPart queryPart =
                parseStatement(statement, query, null, false);
            if (queryPart instanceof Query) {
                ((Query) queryPart).setOwnStatement(true);
                statement = null;
            }
            return queryPart;
        } finally {
            Locus.pop(locus);
            if (statement != null) {
                statement.close();
            }
        }
    }

    public Exp parseExpression(String expr) {
        boolean debug = false;
        if (getLogger().isDebugEnabled()) {
            //debug = true;
            getLogger().debug(
                Util.nl
                + expr);
        }
        final Statement statement = getInternalStatement();
        try {
            MdxParserValidator parser = createParser();
            final FunTable funTable = getSchema().getFunTable();
            return parser.parseExpression(statement, expr, debug, funTable);
        } catch (Throwable exception) {
            throw MondrianResource.instance().FailedToParseQuery.ex(
                expr,
                exception);
        }
    }

    public Statement getInternalStatement() {
        if (internalStatement == null) {
            return schema.getInternalConnection().getInternalStatement();
        } else {
            return internalStatement;
        }
    }

    private Statement createInternalStatement(boolean reentrant) {
        final Statement statement =
            reentrant
                ? new ReentrantInternalStatement()
                : new InternalStatement();
        server.addStatement(statement);
        return statement;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    /**
     * Helper method to allow olap4j wrappers to implement
     * {@link org.olap4j.OlapConnection#createScenario()}.
     *
     * @return new Scenario
     */
    public ScenarioImpl createScenario() {
        return Locus.execute(
            this,
            "createScenario",
            new Locus.Action<ScenarioImpl>() {
                public ScenarioImpl execute() {
                    final ScenarioImpl scenario = new ScenarioImpl();
                    scenario.register(schema);
                    return scenario;
                }
            }
        );
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

        /**
         * Creates a NonEmptyResult.
         *
         * @param result Result set
         * @param execution Execution context
         * @param axis Which axis to make non-empty
         */
        NonEmptyResult(Result result, Execution execution, int axis) {
            super(execution, result.getAxes().clone());

            this.underlying = result;
            this.axis = axis;
            this.map = new HashMap<Integer, Integer>();
            int axisCount = underlying.getAxes().length;
            this.pos = new int[axisCount];
            this.slicerAxis = underlying.getSlicerAxis();
            TupleList tupleList =
                ((RolapAxis) underlying.getAxes()[axis]).getTupleList();

            final TupleList filteredTupleList;
            filteredTupleList =
                TupleCollections.createList(tupleList.getArity());
            int i = -1;
            TupleCursor tupleCursor = tupleList.tupleCursor();
            while (tupleCursor.forward()) {
                ++i;
                if (! isEmpty(i, axis)) {
                    map.put(filteredTupleList.size(), i);
                    filteredTupleList.addCurrent(tupleCursor);
                }
            }
            this.axes[axis] = new RolapAxis(filteredTupleList);
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
                final int positionCount = positions.size();
                for (int i = 0; i < positionCount; i++) {
                    pos[axis] = i;
                    if (!isEmptyRecurse(fixedAxis, axis - 1)) {
                        return false;
                    }
                }
                return true;
            }
        }

        // synchronized because we use 'pos'
        public synchronized Cell getCell(int[] externalPos) {
            try {
                System.arraycopy(
                    externalPos, 0, this.pos, 0, externalPos.length);
                int offset = externalPos[axis];
                int mappedOffset = mapOffsetToUnderlying(offset);
                this.pos[axis] = mappedOffset;
                return underlying.getCell(this.pos);
            } catch (NullPointerException npe) {
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
     * <p>Implementation of {@link Statement} for use when you don't have an
     * olap4j connection.</p>
     */
    private class InternalStatement extends StatementImpl {
        private boolean closed = false;

        public void close() {
            if (!closed) {
                closed = true;
                server.removeStatement(this);
            }
        }

        public RolapConnection getMondrianConnection() {
            return RolapConnection.this;
        }
    }

    /**
     * <p>A statement that can be used for all of the various internal
     * operations, such as resolving MDX identifiers, that require a
     * {@link Statement} and an {@link Execution}.
     *
     * <p>The statement needs to be reentrant because there are many such
     * operations; several of these operations might be active at one time. We
     * don't want to create a new statement for each, but just one internal
     * statement for each connection. The statement shouldn't have a unique
     * execution. For this reason, we don't use the inherited {@link #execution}
     * field.</p>
     *
     * <p>But there is a drawback. If we can't find the unique execution, the
     * statement cannot be canceled or time out. If you want that behavior
     * from an internal statement, use the base class: create a new
     * {@link InternalStatement} for each operation.</p>
     */
    private class ReentrantInternalStatement extends InternalStatement {
        @Override
        public void start(Execution execution) {
            // Unlike StatementImpl, there is not a unique execution. An
            // internal statement can execute several at the same time. So,
            // we don't set this.execution.
            execution.start();
        }

        @Override
        public void end(Execution execution) {
            execution.end();
        }

        @Override
        public void close() {
            // do not close
        }
    }
}

// End RolapConnection.java
