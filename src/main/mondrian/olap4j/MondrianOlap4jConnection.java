/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2007-2010 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap4j;

import mondrian.mdx.*;
import mondrian.olap.*;
import mondrian.olap.Member;
import mondrian.rolap.*;

import mondrian.xmla.XmlaHandler;
import org.olap4j.Axis;
import org.olap4j.Cell;
import org.olap4j.*;
import org.olap4j.impl.*;
import org.olap4j.mdx.*;
import org.olap4j.mdx.parser.*;
import org.olap4j.mdx.parser.impl.DefaultMdxParserImpl;
import org.olap4j.metadata.*;
import org.olap4j.metadata.Schema;
import org.olap4j.type.*;
import org.olap4j.type.DimensionType;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.util.*;

/**
 * Implementation of {@link org.olap4j.OlapConnection}
 * for the Mondrian OLAP engine.
 *
 * <p>This class has sub-classes which implement JDBC 3.0 and JDBC 4.0 APIs;
 * it is instantiated using {@link Factory#newConnection}.</p>
 *
 * @author jhyde
 * @version $Id$
 * @since May 23, 2007
 */
abstract class MondrianOlap4jConnection implements OlapConnection {
    /**
     * Handler for errors.
     */
    final Helper helper = new Helper();

    /**
     * Underlying mondrian connection. Set on creation, cleared on close.
     * Developers, please keep this member private. Access it via
     * {@link #getMondrianConnection()} or {@link #getMondrianConnection2()},
     * and these will throw if the connection has been closed.
     */
    private RolapConnection mondrianConnection;

    /**
     * Current schema.
     */
    MondrianOlap4jSchema olap4jSchema;

    /**
     * Map from mondrian schema objects to olap4j schemas.
     *
     * <p>REVIEW: This assumes that a RolapSchema occurs at most once in a
     * catalog. It is possible for a schema to be mapped more than once, with
     * different names; the same RolapSchema object will be used.
     */
    final Map<mondrian.olap.Schema, MondrianOlap4jSchema> schemaMap =
        new HashMap<mondrian.olap.Schema, MondrianOlap4jSchema>();

    private final MondrianOlap4jDatabaseMetaData olap4jDatabaseMetaData;

    private static final String CONNECT_STRING_PREFIX = "jdbc:mondrian:";

    private static final String ENGINE_CONNECT_STRING_PREFIX =
        "jdbc:mondrian:engine:";

    final Factory factory;
    final MondrianOlap4jDriver driver;
    private Locale locale;
    private String roleName;
    private boolean autoCommit;
    private boolean readOnly;
    boolean preferList;

    /**
     * Creates an Olap4j connection to Mondrian.
     *
     * <p>This method is intentionally package-protected. The public API
     * uses the traditional JDBC {@link java.sql.DriverManager}.
     * See {@link mondrian.olap4j.MondrianOlap4jDriver} for more details.
     *
     * @param factory Factory
     * @param driver Driver
     * @param url Connect-string URL
     * @param info Additional properties
     * @throws SQLException if there is an error
     */
    MondrianOlap4jConnection(
        Factory factory,
        MondrianOlap4jDriver driver,
        String url,
        Properties info)
        throws SQLException
    {
        // Required for the logic below to work.
        assert ENGINE_CONNECT_STRING_PREFIX.startsWith(CONNECT_STRING_PREFIX);

        this.factory = factory;
        this.driver = driver;
        String x;
        if (url.startsWith(ENGINE_CONNECT_STRING_PREFIX)) {
            x = url.substring(ENGINE_CONNECT_STRING_PREFIX.length());
        } else if (url.startsWith(CONNECT_STRING_PREFIX)) {
            x = url.substring(CONNECT_STRING_PREFIX.length());
        } else {
            // This is not a URL we can handle.
            // DriverManager should not have invoked us.
            throw new AssertionError(
                "does not start with '" + CONNECT_STRING_PREFIX + "'");
        }
        Util.PropertyList list = Util.parseConnectString(x);
        final Map<String, String> map = toMap(info);
        for (Map.Entry<String, String> entry : map.entrySet()) {
            list.put(entry.getKey(), entry.getValue());
        }
        this.mondrianConnection =
            (RolapConnection) mondrian.olap.DriverManager
                .getConnection(list, null);
        this.olap4jDatabaseMetaData =
            factory.newDatabaseMetaData(this, mondrianConnection);
        this.olap4jSchema = toOlap4j(mondrianConnection.getSchema());
    }

    static boolean acceptsURL(String url) {
        return url.startsWith(CONNECT_STRING_PREFIX);
    }

    public OlapStatement createStatement() {
        return new MondrianOlap4jStatement(this);
    }

    public ScenarioImpl createScenario() throws OlapException {
        return getMondrianConnection().createScenario();
    }

    public void setScenario(Scenario scenario) throws OlapException {
        getMondrianConnection().setScenario(scenario);
    }

    public Scenario getScenario() throws OlapException {
        return getMondrianConnection().getScenario();
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String nativeSQL(String sql) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException {
        this.autoCommit = autoCommit;
    }

    public boolean getAutoCommit() throws SQLException {
        return autoCommit;
    }

    public void commit() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void rollback() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void close() throws SQLException {
        if (mondrianConnection != null) {
            RolapConnection c = mondrianConnection;
            mondrianConnection = null;
            c.close();
        }
    }

    public boolean isClosed() throws SQLException {
        return mondrianConnection == null;
    }

    public OlapDatabaseMetaData getMetaData() {
        return olap4jDatabaseMetaData;
    }

    @Deprecated
    public NamedList<Catalog> getCatalogs() {
        return olap4jDatabaseMetaData.getCatalogObjects();
    }

    public void setReadOnly(boolean readOnly) throws SQLException {
        this.readOnly = readOnly;
    }

    public boolean isReadOnly() throws SQLException {
        return readOnly;
    }

    public void setCatalog(String catalogName) throws OlapException {
        // ignore
    }

    public String getCatalog() throws OlapException {
        return olap4jSchema.olap4jCatalog.name;
    }

    public void setDatabase(String databaseName) throws OlapException {
        // ignore.
    }

    public String getDatabase() throws OlapException {
        try {
            ResultSet rs = getMetaData().getDatabases();
            rs.first();
            return rs.getString("DATA_SOURCE_NAME");
        } catch (SQLException e) {
            throw helper.createException(
                "Error while querying for the current database name.",
                e);
        }
    }

    public void setTransactionIsolation(int level) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getTransactionIsolation() throws SQLException {
        return TRANSACTION_NONE;
    }

    public SQLWarning getWarnings() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void clearWarnings() throws SQLException {
    }

    public Statement createStatement(
        int resultSetType, int resultSetConcurrency) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public PreparedStatement prepareStatement(
        String sql,
        int resultSetType,
        int resultSetConcurrency) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public CallableStatement prepareCall(
        String sql,
        int resultSetType,
        int resultSetConcurrency) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setHoldability(int holdability) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getHoldability() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Savepoint setSavepoint() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Savepoint setSavepoint(String name) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void rollback(Savepoint savepoint) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Statement createStatement(
        int resultSetType,
        int resultSetConcurrency,
        int resultSetHoldability) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public PreparedStatement prepareStatement(
        String sql,
        int resultSetType,
        int resultSetConcurrency,
        int resultSetHoldability) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public CallableStatement prepareCall(
        String sql,
        int resultSetType,
        int resultSetConcurrency,
        int resultSetHoldability) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public PreparedStatement prepareStatement(
        String sql, int autoGeneratedKeys) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public PreparedStatement prepareStatement(
        String sql, int columnIndexes[]) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public PreparedStatement prepareStatement(
        String sql, String columnNames[]) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement Wrapper

    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        } else if (iface.isInstance(mondrianConnection)) {
            return iface.cast(mondrianConnection);
        }
        if (iface == XmlaHandler.XmlaExtra.class) {
            return iface.cast(MondrianOlap4jExtra.INSTANCE);
        }
        throw helper.createException("does not implement '" + iface + "'");
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this)
            || iface.isInstance(mondrianConnection);
    }

    // implement OlapConnection

    public PreparedOlapStatement prepareOlapStatement(
        String mdx)
        throws OlapException
    {
        return factory.newPreparedStatement(mdx, this);
    }

    public MdxParserFactory getParserFactory() {
        return new MdxParserFactory() {
            public MdxParser createMdxParser(OlapConnection connection) {
                return new DefaultMdxParserImpl();
            }

            public MdxValidator createMdxValidator(OlapConnection connection) {
                return new MondrianOlap4jMdxValidator(connection);
            }
        };
    }

    @Deprecated
    public Schema getSchema() throws OlapException {
        return olap4jSchema;
    }

    MondrianOlap4jCube toOlap4j(mondrian.olap.Cube cube) {
        MondrianOlap4jSchema schema = toOlap4j(cube.getSchema());
        return new MondrianOlap4jCube(cube, schema);
    }

    MondrianOlap4jDimension toOlap4j(mondrian.olap.Dimension dimension) {
        return new MondrianOlap4jDimension(
            toOlap4j(dimension.getSchema()),
            dimension);
    }

    synchronized MondrianOlap4jSchema toOlap4j(
        mondrian.olap.Schema schema)
    {
        MondrianOlap4jSchema olap4jSchema = schemaMap.get(schema);
        if (olap4jSchema == null) {
            throw new RuntimeException("schema not registered: " + schema);
        }
        return olap4jSchema;
    }

    Type toOlap4j(mondrian.olap.type.Type type) {
        if (type instanceof mondrian.olap.type.BooleanType) {
            return new BooleanType();
        } else if (type instanceof mondrian.olap.type.CubeType) {
            final mondrian.olap.Cube mondrianCube =
                ((mondrian.olap.type.CubeType) type).getCube();
            return new CubeType(toOlap4j(mondrianCube));
        } else if (type instanceof mondrian.olap.type.DecimalType) {
            mondrian.olap.type.DecimalType decimalType =
                (mondrian.olap.type.DecimalType) type;
            return new DecimalType(
                decimalType.getPrecision(),
                decimalType.getScale());
        } else if (type instanceof mondrian.olap.type.DimensionType) {
            mondrian.olap.type.DimensionType dimensionType =
                (mondrian.olap.type.DimensionType) type;
            return new DimensionType(
                toOlap4j(dimensionType.getDimension()));
        } else if (type instanceof mondrian.olap.type.HierarchyType) {
            return new BooleanType();
        } else if (type instanceof mondrian.olap.type.LevelType) {
            return new BooleanType();
        } else if (type instanceof mondrian.olap.type.MemberType) {
            final mondrian.olap.type.MemberType memberType =
                (mondrian.olap.type.MemberType) type;
            return new MemberType(
                toOlap4j(memberType.getDimension()),
                toOlap4j(memberType.getHierarchy()),
                toOlap4j(memberType.getLevel()),
                toOlap4j(memberType.getMember()));
        } else if (type instanceof mondrian.olap.type.NullType) {
            return new NullType();
        } else if (type instanceof mondrian.olap.type.NumericType) {
            return new NumericType();
        } else if (type instanceof mondrian.olap.type.SetType) {
            final mondrian.olap.type.SetType setType =
                (mondrian.olap.type.SetType) type;
            return new SetType(toOlap4j(setType.getElementType()));
        } else if (type instanceof mondrian.olap.type.StringType) {
            return new StringType();
        } else if (type instanceof mondrian.olap.type.TupleType) {
            mondrian.olap.type.TupleType tupleType =
                (mondrian.olap.type.TupleType) type;
            final Type[] types = toOlap4j(tupleType.elementTypes);
            return new TupleType(types);
        } else if (type instanceof mondrian.olap.type.SymbolType) {
            return new SymbolType();
        } else {
            throw new UnsupportedOperationException();
        }
    }

    MondrianOlap4jMember toOlap4j(mondrian.olap.Member member) {
        if (member == null) {
            return null;
        }
        if (member instanceof RolapMeasure) {
            RolapMeasure measure = (RolapMeasure) member;
            return new MondrianOlap4jMeasure(
                toOlap4j(member.getDimension().getSchema()),
                measure);
        }
        return new MondrianOlap4jMember(
            toOlap4j(member.getDimension().getSchema()),
            member);
    }

    MondrianOlap4jLevel toOlap4j(mondrian.olap.Level level) {
        if (level == null) {
            return null;
        }
        return new MondrianOlap4jLevel(
            toOlap4j(level.getDimension().getSchema()),
            level);
    }

    MondrianOlap4jHierarchy toOlap4j(mondrian.olap.Hierarchy hierarchy) {
        if (hierarchy == null) {
            return null;
        }
        return new MondrianOlap4jHierarchy(
            toOlap4j(hierarchy.getDimension().getSchema()),
            hierarchy);
    }

    Type[] toOlap4j(mondrian.olap.type.Type[] mondrianTypes) {
        final Type[] types = new Type[mondrianTypes.length];
        for (int i = 0; i < types.length; i++) {
            types[i] = toOlap4j(mondrianTypes[i]);
        }
        return types;
    }

    NamedList<MondrianOlap4jMember> toOlap4j(
        final List<Member> memberList)
    {
        return new AbstractNamedList<MondrianOlap4jMember>() {
            protected String getName(MondrianOlap4jMember olap4jMember) {
                return olap4jMember.getName();
            }

            public MondrianOlap4jMember get(int index) {
                return toOlap4j(memberList.get(index));
            }

            public int size() {
                return memberList.size();
            }
        };
    }
    /**
     * Converts a Properties object to a Map with String keys and values.
     *
     * @param properties Properties
     * @return Map backed by the given Properties object
     */
    public static Map<String, String> toMap(final Properties properties) {
        return new AbstractMap<String, String>() {
            public Set<Entry<String, String>> entrySet() {
                return Olap4jUtil.cast(properties.entrySet());
            }
        };
    }

    MondrianOlap4jNamedSet toOlap4j(
        mondrian.olap.Cube cube,
        mondrian.olap.NamedSet namedSet)
    {
        if (namedSet == null) {
            return null;
        }
        return new MondrianOlap4jNamedSet(
            toOlap4j(cube),
            namedSet);
    }

    ParseTreeNode toOlap4j(Exp exp) {
        return new MondrianToOlap4jNodeConverter(this).toOlap4j(exp);
    }

    SelectNode toOlap4j(Query query) {
        return new MondrianToOlap4jNodeConverter(this).toOlap4j(query);
    }

    public void setLocale(Locale locale) {
        if (locale == null) {
            throw new IllegalArgumentException("locale must not be null");
        }
        this.locale = locale;
    }

    public Locale getLocale() {
        if (locale == null) {
            return Locale.getDefault();
        }
        return locale;
    }

    public void setRoleName(String roleName) throws OlapException {
        final Role role;
        final RolapConnection connection1 = getMondrianConnection();
        if (roleName == null) {
            role = ((RolapSchema) connection1.getSchema())
                .getInternalConnection().getRole();
            assert role != null;
        } else {
            role = connection1.getSchema().lookupRole(roleName);
            if (role == null) {
                throw helper.createException("Unknown role '" + roleName + "'");
            }
        }
        // Remember the name of the role, because mondrian roles don't know
        // their own name.
        this.roleName = roleName;
        connection1.setRole(role);
    }

    public String getRoleName() {
        return roleName;
    }

    public List<String> getAvailableRoleNames() throws OlapException {
        return UnmodifiableArrayList.of(
            ((RolapSchema) getMondrianConnection().getSchema()).roleNames());
    }

    public void setPreferList(boolean preferList) {
        this.preferList = preferList;
    }

    /**
     * Cop-out version of {@link #getMondrianConnection()} that doesn't throw
     * a checked exception. For those situations where the olap4j API doesn't
     * declare 'throws OlapException', but we need an open connection anyway.
     * Use {@link #getMondrianConnection()} where possible.
     *
     * @return Mondrian connection
     * @throws RuntimeException if connection is closed
     */
    RolapConnection getMondrianConnection2() throws RuntimeException {
        try {
            return getMondrianConnection();
        } catch (OlapException e) {
            // Demote from checked to unchecked exception.
            throw new RuntimeException(e);
        }
    }

    RolapConnection getMondrianConnection() throws OlapException {
        final RolapConnection connection1 = mondrianConnection;
        if (connection1 == null) {
            throw helper.createException("Connection is closed.");
        }
        return connection1;
    }

    /**
     * Returns an unmodifiable view of the specified list.  This method allows
     * modules to provide users with "read-only" access to internal
     * lists.  Query operations on the returned list "read through" to the
     * specified list, and attempts to modify the returned list, whether
     * direct or via its iterator, result in an
     * <tt>UnsupportedOperationException</tt>.<p>
     *
     * The returned list will be serializable if the specified list
     * is serializable. Similarly, the returned list will implement
     * {@link RandomAccess} if the specified list does.
     *
     * <p>The equivalent of
     * {@link java.util.Collections#unmodifiableList(java.util.List)}.
     *
     * @param  list the list for which an unmodifiable view is to be returned.
     * @return an unmodifiable view of the specified list.
     */
    static <T> NamedList<T> unmodifiableNamedList(
        final NamedList<? extends T> list)
    {
        return list instanceof RandomAccess
        ? new UnmodifiableNamedRandomAccessList<T>(list)
        : new UnmodifiableNamedList<T>(list);
    }

    // inner classes

    /**
     * Package-private helper class which encapsulates policies which are
     * common throughout the driver. These policies include exception handling
     * and factory methods.
     */
    static class Helper {
        OlapException createException(String msg) {
            return new OlapException(msg);
        }

        /**
         * Creates an exception in the context of a particular Cell.
         *
         * @param context Cell context for exception
         * @param msg Message
         * @return New exception
         */
        OlapException createException(Cell context, String msg) {
            OlapException exception = new OlapException(msg);
            exception.setContext(context);
            return exception;
        }

        /**
         * Creates an exception in the context of a particular Cell and with
         * a given cause.
         *
         * @param context Cell context for exception
         * @param msg Message
         * @param cause Causing exception
         * @return New exception
         */
        OlapException createException(
            Cell context, String msg, Throwable cause)
        {
            OlapException exception = new OlapException(msg, cause);
            exception.setContext(context);
            return exception;
        }

        /**
         * Creates an exception with a given cause.
         *
         * @param msg Message
         * @param cause Causing exception
         * @return New exception
         */
        OlapException createException(
            String msg, Throwable cause)
        {
            return new OlapException(msg, cause);
        }

        /**
         * Converts a SQLException to an OlapException. Casts the exception
         * if it is already an OlapException, wraps otherwise.
         *
         * <p>This method is typically used as an adapter for SQLException
         * instances coming from a base class, where derived interface declares
         * that it throws the more specific OlapException.
         *
         * @param e Exception
         * @return Exception as an OlapException
         */
        public OlapException toOlapException(SQLException e) {
            if (e instanceof OlapException) {
                return (OlapException) e;
            } else {
                return new OlapException(null, e);
            }
        }
    }

    private static class MondrianOlap4jMdxValidator implements MdxValidator {
        private final MondrianOlap4jConnection connection;

        public MondrianOlap4jMdxValidator(OlapConnection connection) {
            this.connection = (MondrianOlap4jConnection) connection;
        }

        public SelectNode validateSelect(SelectNode selectNode)
            throws OlapException
        {
            try {
                // A lot of mondrian's validation happens during parsing.
                // Therefore to do effective validation, we need to go back to
                // the MDX string. Someday we will reshape mondrian's
                // parse/validation process to fit the olap4j model better.
                StringWriter sw = new StringWriter();
                selectNode.unparse(new ParseTreeWriter(new PrintWriter(sw)));
                String mdx = sw.toString();
                Query query =
                    connection.mondrianConnection
                        .parseQuery(mdx);
                query.resolve();
                return connection.toOlap4j(query);
            } catch (MondrianException e) {
                throw connection.helper.createException("Validation error", e);
            }
        }
    }

    private static class MondrianToOlap4jNodeConverter {
        private final MondrianOlap4jConnection olap4jConnection;

        MondrianToOlap4jNodeConverter(
            MondrianOlap4jConnection olap4jConnection)
        {
            this.olap4jConnection = olap4jConnection;
        }

        public SelectNode toOlap4j(Query query) {
            List<IdentifierNode> list = Collections.emptyList();
            return new SelectNode(
                null,
                toOlap4j(query.getFormulas()),
                toOlap4j(query.getAxes()),
                new CubeNode(
                    null,
                    olap4jConnection.toOlap4j(query.getCube())),
                query.getSlicerAxis() == null
                    ? null
                    : toOlap4j(query.getSlicerAxis()),
                list);
        }

        private AxisNode toOlap4j(QueryAxis axis) {
            return new AxisNode(
                null,
                axis.isNonEmpty(),
                Axis.Factory.forOrdinal(
                    axis.getAxisOrdinal().logicalOrdinal()),
                toOlap4j(axis.getDimensionProperties()),
                toOlap4j(axis.getSet()));
        }

        private List<IdentifierNode> toOlap4j(Id[] dimensionProperties) {
            final List<IdentifierNode> list = new ArrayList<IdentifierNode>();
            for (Id property : dimensionProperties) {
                list.add(toOlap4j(property));
            }
            return list;
        }

        private ParseTreeNode toOlap4j(Exp exp) {
            if (exp instanceof Id) {
                Id id = (Id) exp;
                return toOlap4j(id);
            }
            if (exp instanceof ResolvedFunCall) {
                ResolvedFunCall call = (ResolvedFunCall) exp;
                return toOlap4j(call);
            }
            if (exp instanceof DimensionExpr) {
                DimensionExpr dimensionExpr = (DimensionExpr) exp;
                return new DimensionNode(
                    null,
                    olap4jConnection.toOlap4j(dimensionExpr.getDimension()));
            }
            if (exp instanceof HierarchyExpr) {
                HierarchyExpr hierarchyExpr = (HierarchyExpr) exp;
                return new HierarchyNode(
                    null,
                    olap4jConnection.toOlap4j(hierarchyExpr.getHierarchy()));
            }
            if (exp instanceof LevelExpr) {
                LevelExpr levelExpr = (LevelExpr) exp;
                return new LevelNode(
                    null,
                    olap4jConnection.toOlap4j(levelExpr.getLevel()));
            }
            if (exp instanceof MemberExpr) {
                MemberExpr memberExpr = (MemberExpr) exp;
                return new MemberNode(
                    null,
                    olap4jConnection.toOlap4j(memberExpr.getMember()));
            }
            if (exp instanceof Literal) {
                Literal literal = (Literal) exp;
                final Object value = literal.getValue();
                if (literal.getCategory() == Category.Symbol) {
                    return LiteralNode.createSymbol(
                        null, (String) literal.getValue());
                } else if (value instanceof Number) {
                    Number number = (Number) value;
                    BigDecimal bd = bigDecimalFor(number);
                    return LiteralNode.createNumeric(null, bd, false);
                } else if (value instanceof String) {
                    return LiteralNode.createString(null, (String) value);
                } else if (value == null) {
                    return LiteralNode.createNull(null);
                } else {
                    throw new RuntimeException("unknown literal " + literal);
                }
            }
            throw Util.needToImplement(exp.getClass());
        }

        /**
         * Converts a number to big decimal, non-lossy if possible.
         *
         * @param number Number
         * @return BigDecimal
         */
        private static BigDecimal bigDecimalFor(Number number) {
            if (number instanceof BigDecimal) {
                return (BigDecimal) number;
            } else if (number instanceof BigInteger) {
                return new BigDecimal((BigInteger) number);
            } else if (number instanceof Integer) {
                return new BigDecimal((Integer) number);
            } else if (number instanceof Double) {
                return new BigDecimal((Double) number);
            } else if (number instanceof Float) {
                return new BigDecimal((Float) number);
            } else if (number instanceof Long) {
                return new BigDecimal((Long) number);
            } else if (number instanceof Short) {
                return new BigDecimal((Short) number);
            } else if (number instanceof Byte) {
                return new BigDecimal((Byte) number);
            } else {
                return new BigDecimal(number.doubleValue());
            }
        }

        private ParseTreeNode toOlap4j(ResolvedFunCall call) {
            final CallNode callNode = new CallNode(
                null,
                call.getFunName(),
                toOlap4j(call.getSyntax()),
                toOlap4j(Arrays.asList(call.getArgs())));
            if (call.getType() != null) {
                callNode.setType(olap4jConnection.toOlap4j(call.getType()));
            }
            return callNode;
        }

        private List<ParseTreeNode> toOlap4j(List<Exp> exprList) {
            final List<ParseTreeNode> result = new ArrayList<ParseTreeNode>();
            for (Exp expr : exprList) {
                result.add(toOlap4j(expr));
            }
            return result;
        }

        private org.olap4j.mdx.Syntax toOlap4j(mondrian.olap.Syntax syntax) {
            return org.olap4j.mdx.Syntax.valueOf(syntax.name());
        }

        private List<AxisNode> toOlap4j(QueryAxis[] axes) {
            final ArrayList<AxisNode> axisList = new ArrayList<AxisNode>();
            for (QueryAxis axis : axes) {
                axisList.add(toOlap4j(axis));
            }
            return axisList;
        }

        private List<ParseTreeNode> toOlap4j(Formula[] formulas) {
            final List<ParseTreeNode> list = new ArrayList<ParseTreeNode>();
            for (Formula formula : formulas) {
                if (formula.isMember()) {
                    List<PropertyValueNode> memberPropertyList =
                        new ArrayList<PropertyValueNode>();
                    for (Object child : formula.getChildren()) {
                        if (child instanceof MemberProperty) {
                            MemberProperty memberProperty =
                                (MemberProperty) child;
                            memberPropertyList.add(
                                new PropertyValueNode(
                                    null,
                                    memberProperty.getName(),
                                    toOlap4j(memberProperty.getExp())));
                        }
                    }
                    list.add(
                        new WithMemberNode(
                            null,
                            toOlap4j(formula.getIdentifier()),
                            toOlap4j(formula.getExpression()),
                            memberPropertyList));
                }
            }
            return list;
        }

        private IdentifierNode toOlap4j(Id id) {
            List<IdentifierSegment> list =
                new ArrayList<IdentifierSegment>();
            for (Id.Segment segment : id.getSegments()) {
                list.add(
                    new NameSegment(
                        null,
                        segment.name,
                        toOlap4j(segment.quoting)));
            }
            return new IdentifierNode(
                list.toArray(
                    new IdentifierSegment[list.size()]));
        }

        private Quoting toOlap4j(Id.Quoting quoting) {
            return Quoting.valueOf(quoting.name());
        }
    }

    private static class UnmodifiableNamedList<T> implements NamedList<T> {
        private final NamedList<? extends T> list;

        UnmodifiableNamedList(NamedList<? extends T> list) {
            this.list = list;
        }

        public T get(String s) {
            return list.get(s);
        }

        public int indexOfName(String s) {
            return list.indexOfName(s);
        }

        public int size() {
            return list.size();
        }

        public boolean isEmpty() {
            return list.isEmpty();
        }

        public boolean contains(Object o) {
            return list.contains(o);
        }

        public Iterator<T> iterator() {
            return Collections.unmodifiableList(list).iterator();
        }

        public Object[] toArray() {
            return list.toArray();
        }

        public <T2> T2[] toArray(T2[] a) {
            //noinspection SuspiciousToArrayCall
            return list.toArray(a);
        }

        public boolean add(T t) {
            throw new UnsupportedOperationException();
        }

        public boolean remove(Object o) {
            throw new UnsupportedOperationException();
        }

        public boolean containsAll(Collection<?> c) {
            throw new UnsupportedOperationException();
        }

        public boolean addAll(Collection<? extends T> c) {
            throw new UnsupportedOperationException();
        }

        public boolean addAll(int index, Collection<? extends T> c) {
            throw new UnsupportedOperationException();
        }

        public boolean removeAll(Collection<?> c) {
            throw new UnsupportedOperationException();
        }

        public boolean retainAll(Collection<?> c) {
            throw new UnsupportedOperationException();
        }

        public void clear() {
            throw new UnsupportedOperationException();
        }

        public T get(int index) {
            return list.get(index);
        }

        public T set(int index, T element) {
            throw new UnsupportedOperationException();
        }

        public void add(int index, T element) {
            throw new UnsupportedOperationException();
        }

        public T remove(int index) {
            throw new UnsupportedOperationException();
        }

        public int indexOf(Object o) {
            return list.indexOf(0);
        }

        public int lastIndexOf(Object o) {
            return list.lastIndexOf(o);
        }

        public ListIterator<T> listIterator() {
            return Collections.unmodifiableList(list).listIterator();
        }

        public ListIterator<T> listIterator(int index) {
            return Collections.unmodifiableList(list).listIterator(index);
        }

        public List<T> subList(int fromIndex, int toIndex) {
            // TODO: NamedList.subList should return NamedList.
            return Collections.unmodifiableList(
                list.subList(fromIndex, toIndex));
        }
    }

    private static class UnmodifiableNamedRandomAccessList<T>
        extends UnmodifiableNamedList<T>
        implements RandomAccess
    {
        UnmodifiableNamedRandomAccessList(NamedList<? extends T> list) {
            super(list);
        }
    }
}

// End MondrianOlap4jConnection.java
