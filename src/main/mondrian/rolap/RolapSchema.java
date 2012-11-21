/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2012 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.olap.Member;
import mondrian.olap.fun.*;
import mondrian.olap.type.Type;
import mondrian.resource.MondrianResource;
import mondrian.rolap.aggmatcher.AggTableManager;
import mondrian.rolap.aggmatcher.JdbcSchema;
import mondrian.rolap.sql.SqlQuery;
import mondrian.server.*;
import mondrian.server.Statement;
import mondrian.spi.*;
import mondrian.spi.impl.*;
import mondrian.util.*;

import org.apache.log4j.Logger;

import org.eigenbase.xom.*;

import org.olap4j.impl.*;
import org.olap4j.mdx.IdentifierSegment;
import org.olap4j.metadata.NamedList;

import java.lang.reflect.*;
import java.sql.*;
import java.util.*;
import java.util.Date;

import javax.sql.DataSource;

/**
 * A <code>RolapSchema</code> is a collection of {@link RolapCube}s and
 * shared {@link RolapDimension}s. It is shared between {@link
 * RolapConnection}s. It caches {@link MemberReader}s, etc.
 *
 * @see RolapConnection
 * @author jhyde
 * @since 26 July, 2001
 */
public class RolapSchema extends OlapElementBase implements Schema {
    static final Logger LOGGER = Logger.getLogger(RolapSchema.class);

    final String name;

    /**
     * Internal use only.
     */
    private RolapConnection internalConnection;

    private final Dialect dialect;

    /**
     * Holds cubes in this schema.
     */
    private final Map<String, RolapCube> mapNameToCube =
        new HashMap<String, RolapCube>();

    /**
     * Maps {@link String shared hierarchy name} to {@link MemberReader}.
     * Shared between all statements which use this connection.
     */
    private final Map<String, MemberReader> mapSharedHierarchyToReader =
        new HashMap<String, MemberReader>();

    /**
     * Maps {@link String names of shared hierarchies} to {@link
     * RolapHierarchy the canonical instance of those hierarchies}.
     */
    private final Map<String, RolapHierarchy> mapSharedHierarchyNameToHierarchy
        =
        new HashMap<String, RolapHierarchy>();

    /**
     * The default role for connections to this schema.
     */
    private Role defaultRole;

    ByteString md5Bytes;

    /**
     * A schema's aggregation information
     */
    AggTableManager aggTableManager;

    /**
     * This is basically a unique identifier for this RolapSchema instance
     * used it its equals and hashCode methods.
     */
    final SchemaKey key;

    /**
     * Maps {@link String names of roles} to {@link Role roles with those names}.
     */
    private final Map<String, Role> mapNameToRole = new HashMap<String, Role>();

    /**
     * Maps {@link String names of sets} to {@link NamedSet named sets}.
     */
    private final Map<String, NamedSet> mapNameToSet =
        new HashMap<String, NamedSet>();

    /**
     * Table containing all standard MDX functions, plus user-defined functions
     * for this schema.
     */
    private FunTable funTable;

    final List<RolapSchemaParameter > parameterList =
        new ArrayList<RolapSchemaParameter >();

    private Date schemaLoadDate;

    private DataSourceChangeListener dataSourceChangeListener;

    PhysSchema physicalSchema;

    /**
     * List of warnings. Populated when a schema is created by a connection
     * that has
     * {@link mondrian.rolap.RolapConnectionProperties#Ignore Ignore}=true.
     */
    final List<MondrianSchemaException> warningList;

    private final Map<String, Annotation> annotationMap;

    /**
     * Unique schema instance id that will be used
     * to inform clients when the schema has changed.
     *
     * <p>Expect a different ID for each Mondrian instance node.
     */
    private final String id;

    private final String description;

    /**
     * Creates a schema.
     *
     * <p>Must be called from {@link mondrian.rolap.RolapSchemaLoader}, and
     * then immediately loaded. Never directly from the pool.
     *
     * @param key Key
     * @param connectInfo Connect properties
     * @param dataSource Data source
     * @param md5Bytes MD5 hash
     * @param useContentChecksum Whether to use content checksum
     * @param name Name
     * @param caption Caption
     * @param description Description
     * @param quoteSql Whether dialect should not quote SQL identifiers
     * @param annotationMap Annotation map
     */
    RolapSchema(
        final SchemaKey key,
        final Util.PropertyList connectInfo,
        final DataSource dataSource,
        final ByteString md5Bytes,
        boolean useContentChecksum,
        String name,
        String caption,
        String description,
        boolean quoteSql,
        Map<String, Annotation> annotationMap)
    {
        this.id = Util.generateUuidString();
        this.caption = caption;
        this.description = description;
        this.key = key;
        this.md5Bytes = md5Bytes;
        if (useContentChecksum && md5Bytes == null) {
            throw new AssertionError();
        }

        // the order of the next two lines is important
        this.defaultRole = Util.createRootRole(this);
        final MondrianServer internalServer = MondrianServer.forId(null);
        this.internalConnection =
            new RolapConnection(internalServer, connectInfo, this, dataSource);
        assert internalConnection.dialect != null;
        internalServer.removeConnection(internalConnection);
        internalServer.removeStatement(
            internalConnection.getInternalStatement());
        this.dialect = internalConnection.dialect.withQuoting(quoteSql);

        this.aggTableManager = new AggTableManager(this);
        this.dataSourceChangeListener =
            createDataSourceChangeListener(connectInfo);
        this.name = name;
        if (name == null || name.equals("")) {
            throw Util.newError("<Schema> name must be set");
        }
        this.annotationMap = annotationMap;

        if (getInternalConnection() != null
            && "true".equals(
                getInternalConnection().getProperty(
                    RolapConnectionProperties.Ignore.name())))
        {
            warningList = new ArrayList<MondrianSchemaException>();
        } else {
            warningList = null;
        }
    }

    public String getUniqueName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public OlapElement lookupChild(
        SchemaReader schemaReader, Id.Segment s, MatchType matchType)
    {
        throw new UnsupportedOperationException();
    }

    public String getQualifiedName() {
        return name;
    }

    public Hierarchy getHierarchy() {
        throw new UnsupportedOperationException();
    }

    public Dimension getDimension() {
        throw new UnsupportedOperationException();
    }

    protected void finalCleanUp() {
        if (aggTableManager != null) {
            aggTableManager.finalCleanUp();
            aggTableManager = null;
        }
    }

    protected void finalize() throws Throwable {
        try {
            super.finalize();
            finalCleanUp();
        } catch (Throwable t) {
            LOGGER.info(
                MondrianResource.instance()
                    .FinalizerErrorRolapSchema.baseMessage,
                t);
        }
    }

    public boolean equals(Object o) {
        if (!(o instanceof RolapSchema)) {
            return false;
        }
        RolapSchema other = (RolapSchema) o;
        return other.key.equals(key);
    }

    public int hashCode() {
        return key.hashCode();
    }

    protected Logger getLogger() {
        return LOGGER;
    }

    void setSchemaLoadDate() {
        schemaLoadDate = new Date();
    }

    public Date getSchemaLoadDate() {
        return schemaLoadDate;
    }

    public List<Exception> getWarnings() {
        return Collections.unmodifiableList(Util.<Exception>cast(warningList));
    }

    public Role getDefaultRole() {
        return defaultRole;
    }

    public String getName() {
        return name;
    }

    /**
     * Returns this schema instance unique ID.
     * @return A string representing the schema ID.
     */
    public String getId() {
        return this.id;
    }

    public Map<String, Annotation> getAnnotationMap() {
        return annotationMap;
    }

    /**
     * Returns this schema's SQL dialect.
     *
     * <p>NOTE: This method is not cheap. The implementation gets a connection
     * from the connection pool.
     *
     * @return dialect
     */
    public Dialect getDialect() {
        if (true) {
            Util.deprecated("clean up", false);
            return dialect;
        }
        DataSource dataSource = getInternalConnection().getDataSource();
        return DialectManager.createDialect(dataSource, null);
    }

    public Dimension createDimension(Cube cube, String xml) {
        throw new UnsupportedOperationException(
            "Support for Schema.createDimension has been removed in Mondrian 4.0");
    }

    public Cube createCube(String xml) {
        return new RolapSchemaLoader(this).createCube(xml);
    }

    public PhysSchema getPhysicalSchema() {
        return physicalSchema;
    }

    public static List<RolapSchema> getRolapSchemas() {
        return RolapSchemaPool.instance().getRolapSchemas();
    }

    public static boolean cacheContains(RolapSchema rolapSchema) {
        return RolapSchemaPool.instance().contains(rolapSchema);
    }

    public Cube lookupCube(final String cube, final boolean failIfNotFound) {
        RolapCube mdxCube = lookupCube(cube);
        if (mdxCube == null && failIfNotFound) {
            throw MondrianResource.instance().MdxCubeNotFound.ex(cube);
        }
        return mdxCube;
    }

    /**
     * Finds a cube called 'cube' in the current catalog, or return null if no
     * cube exists.
     */
    protected RolapCube lookupCube(final String cubeName) {
        return mapNameToCube.get(Util.normalizeName(cubeName));
    }

    /**
     * Returns a list of cubes that have at least one measure in the given
     * star.
     *
     * @param star Star
     * @return List of cubes whose measures are in the given star
     */
    public List<RolapCube> getCubesWithStar(RolapStar star) {
        List<RolapCube> list = new ArrayList<RolapCube>();
        cubeLoop:
        for (RolapCube cube : mapNameToCube.values()) {
            for (Member member : cube.getMeasures()) {
                if (member instanceof RolapStoredMeasure) {
                    RolapStoredMeasure measure = (RolapStoredMeasure) member;
                    if (measure.getStarMeasure().getStar() == star) {
                        list.add(cube);
                        continue cubeLoop;
                    }
                }
            }
        }
        return list;
    }

    public NamedSet getNamedSet(IdentifierSegment segment) {
        // FIXME: write a map that efficiently maps segment->value, taking
        // into account case-sensitivity etc.
        for (Map.Entry<String, NamedSet> entry : mapNameToSet.entrySet()) {
            if (Util.matches(segment, entry.getKey())) {
                return entry.getValue();
            }
        }
        return null;
    }

    /**
     * Adds a cube to the cube name map.
     * @see #lookupCube(String)
     */
    protected void addCube(final RolapCube cube) {
        mapNameToCube.put(
            Util.normalizeName(cube.getName()),
            cube);
    }

    protected void addNamedSet(String name, NamedSet namedSet) {
        mapNameToSet.put(name, namedSet);
    }

    public boolean removeCube(final String cubeName) {
        final RolapCube cube =
            mapNameToCube.remove(Util.normalizeName(cubeName));
        return cube != null;
    }

    public Cube[] getCubes() {
        Collection<RolapCube> cubes = mapNameToCube.values();
        return cubes.toArray(new RolapCube[cubes.size()]);
    }

    public NamedList<RolapCube> getCubeList() {
        return new NamedListImpl<RolapCube>(mapNameToCube.values());
    }

    public Hierarchy[] getSharedHierarchies() {
        Collection<RolapHierarchy> hierarchies =
            mapSharedHierarchyNameToHierarchy.values();
        return hierarchies.toArray(new RolapHierarchy[hierarchies.size()]);
    }

    RolapHierarchy getSharedHierarchy(final String name) {
        return mapSharedHierarchyNameToHierarchy.get(name);
    }

    public NamedSet getNamedSet(String name) {
        return mapNameToSet.get(name);
    }

    public Role lookupRole(final String role) {
        return mapNameToRole.get(role);
    }

    public Set<String> roleNames() {
        return mapNameToRole.keySet();
    }

    public FunTable getFunTable() {
        return funTable;
    }

    public Parameter[] getParameters() {
        return parameterList.toArray(
            new Parameter[parameterList.size()]);
    }

    /**
     * Defines a user-defined function in this table.
     *
     * <p>If the function is not valid, throws an error.
     *
     * @param name Name of the function.
     * @param className Name of the class which implements the function.
     *   The class must implement {@link mondrian.spi.UserDefinedFunction}
     *   (otherwise it is a user-error).
     * @param script Script
     */
    void defineFunction(
        Map<String, UdfResolver.UdfFactory> mapNameToUdf,
        final String name,
        String className,
        final Scripts.ScriptDefinition script)
    {
        if (className == null && script == null) {
            throw Util.newError(
                "Must specify either className attribute or Script element");
        }
        if (className != null && script != null) {
            throw Util.newError(
                "Must not specify both className attribute and Script element");
        }
        final UdfResolver.UdfFactory udfFactory;
        if (className != null) {
            // Lookup class.
            final Class<UserDefinedFunction> klass;
            try {
                //noinspection unchecked
                klass = (Class<UserDefinedFunction>) Class.forName(className);
            } catch (ClassNotFoundException e) {
                throw MondrianResource.instance().UdfClassNotFound.ex(
                    name,
                    className);
            }
            // Instantiate UDF by calling correct constructor.
            udfFactory = new UdfResolver.ClassUdfFactory(klass, name);
        } else {
            udfFactory = new UdfResolver.UdfFactory() {
                public UserDefinedFunction create() {
                    return Scripts.userDefinedFunction(script, name);
                }
            };
        }
        // Validate function.
        validateFunction(udfFactory);
        // Check for duplicate.
        UdfResolver.UdfFactory existingUdf = mapNameToUdf.get(name);
        if (existingUdf != null) {
            throw MondrianResource.instance().UdfDuplicateName.ex(name);
        }
        mapNameToUdf.put(name, udfFactory);
    }

    /**
     * Throws an error if a user-defined function does not adhere to the
     * API.
     */
    private void validateFunction(final UdfResolver.UdfFactory udfFactory) {
        final UserDefinedFunction udf = udfFactory.create();

        // Check that the name is not null or empty.
        final String udfName = udf.getName();
        if (udfName == null || udfName.equals("")) {
            throw Util.newInternal(
                "User-defined function defined by class '"
                + udf.getClass() + "' has empty name");
        }
        // It's OK for the description to be null.
        final String description = udf.getDescription();
        Util.discard(description);
        final Type[] parameterTypes = udf.getParameterTypes();
        for (int i = 0; i < parameterTypes.length; i++) {
            Type parameterType = parameterTypes[i];
            if (parameterType == null) {
                throw Util.newInternal(
                    "Invalid user-defined function '"
                    + udfName + "': parameter type #" + i + " is null");
            }
        }
        // It's OK for the reserved words to be null or empty.
        final String[] reservedWords = udf.getReservedWords();
        Util.discard(reservedWords);
        // Test that the function returns a sensible type when given the FORMAL
        // types. It may still fail when we give it the ACTUAL types, but it's
        // impossible to check that now.
        final Type returnType = udf.getReturnType(parameterTypes);
        if (returnType == null) {
            throw Util.newInternal(
                "Invalid user-defined function '"
                + udfName + "': return type is null");
        }
        final Syntax syntax = udf.getSyntax();
        if (syntax == null) {
            throw Util.newInternal(
                "Invalid user-defined function '"
                + udfName + "': syntax is null");
        }
    }

    /**
     * Gets a {@link MemberReader} with which to read a hierarchy. If the
     * hierarchy is shared (<code>sharedName</code> is not null), looks up
     * a reader from a cache, or creates one if necessary.
     *
     * <p>Synchronization: thread safe
     */
    synchronized MemberReader createMemberReader(
        final String sharedName,
        final RolapHierarchy hierarchy,
        final String memberReaderClass)
    {
        assert sharedName == null; // TODO: re-enable sharing
        MemberReader reader;
        if (sharedName != null) {
            reader = mapSharedHierarchyToReader.get(sharedName);
        } else {
            reader = null;
        }
        if (reader == null) {
            reader = createMemberReader(hierarchy, memberReaderClass);
        }
        if (sharedName != null) {
            // share, for other uses of the same shared hierarchy
            mapSharedHierarchyNameToHierarchy.put(sharedName, hierarchy);
        }
        return reader;
    }

    /**
     * Creates a {@link MemberReader} with which to Read a hierarchy.
     */
    private MemberReader createMemberReader(
        final RolapHierarchy hierarchy,
        final String memberReaderClass)
    {
        if (memberReaderClass != null) {
            Exception e2;
            try {
                Properties properties = null;
                Class<?> clazz = Class.forName(memberReaderClass);
                Constructor<?> constructor = clazz.getConstructor(
                    RolapHierarchy.class,
                    Properties.class);
                Object o = constructor.newInstance(hierarchy, properties);
                if (o instanceof MemberReader) {
                    return (MemberReader) o;
                } else if (o instanceof MemberSource) {
                    return new CacheMemberReader((MemberSource) o);
                } else {
                    throw Util.newInternal(
                        "member reader class " + clazz
                        + " does not implement " + MemberSource.class);
                }
            } catch (ClassNotFoundException e) {
                e2 = e;
            } catch (NoSuchMethodException e) {
                e2 = e;
            } catch (InstantiationException e) {
                e2 = e;
            } catch (IllegalAccessException e) {
                e2 = e;
            } catch (InvocationTargetException e) {
                e2 = e;
            }
            throw Util.newInternal(
                e2,
                "while instantiating member reader '" + memberReaderClass);
        } else {
            SqlMemberSource source = new SqlMemberSource(hierarchy);
            if (hierarchy.getDimension().isHighCardinality()) {
                LOGGER.debug(
                    "High cardinality for " + hierarchy.getDimension());
                return new NoCacheMemberReader(source);
            } else {
                LOGGER.debug(
                    "Normal cardinality for " + hierarchy.getDimension());
                if (MondrianProperties.instance().DisableCaching.get()) {
                    // If the cell cache is disabled, we can't cache
                    // the members or else we get undefined results,
                    // depending on the functions used and all.
                    return new NoCacheMemberReader(source);
                } else {
                    return new SmartMemberReader(source);
                }
            }
        }
    }

    public SchemaReader getSchemaReader() {
        return new RolapSchemaReader(defaultRole, this).withLocus();
    }

    /**
     * Creates a {@link DataSourceChangeListener} with which to detect changes
     * to datasources.
     */
    private DataSourceChangeListener createDataSourceChangeListener(
        Util.PropertyList connectInfo)
    {
        DataSourceChangeListener changeListener = null;

        // If CatalogContent is specified in the connect string, ignore
        // everything else. In particular, ignore the dynamic schema
        // processor.
        String dataSourceChangeListenerStr = connectInfo.get(
            RolapConnectionProperties.DataSourceChangeListener.name());

        if (!Util.isEmpty(dataSourceChangeListenerStr)) {
            try {
                Class<?> clazz = Class.forName(dataSourceChangeListenerStr);
                Constructor<?> constructor = clazz.getConstructor();
                changeListener =
                    (DataSourceChangeListener) constructor.newInstance();
            } catch (Exception e) {
                throw Util.newError(
                    e,
                    "loading DataSourceChangeListener "
                    + dataSourceChangeListenerStr);
            }

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                    "RolapSchema.createDataSourceChangeListener: "
                    + "create datasource change listener \""
                    + dataSourceChangeListenerStr);
            }
        }
        return changeListener;
    }

    /**
     * Returns the checksum of this schema. Returns
     * <code>null</code> if {@link RolapConnectionProperties#UseContentChecksum}
     * is set to false.
     *
     * @return MD5 checksum of this schema
     */
    public ByteString getChecksum() {
        return md5Bytes;
    }

    /**
     * Connection for purposes of parsing and validation. Careful! It won't
     * have the correct locale or access-control profile.
     */
    public RolapConnection getInternalConnection() {
        return internalConnection;
    }

    private RolapStar makeRolapStar(final PhysRelation fact) {
        DataSource dataSource = getInternalConnection().getDataSource();
        return new RolapStar(this, dataSource, fact);
    }

    void registerRoles(Map<String, Role> roles, Role defaultRole) {
        for (Map.Entry<String, Role> entry : roles.entrySet()) {
            mapNameToRole.put(entry.getKey(), entry.getValue());
        }

        this.defaultRole = defaultRole;
    }

    void initFunctionTable(
        Collection<UdfResolver.UdfFactory> userDefinedFunctions)
    {
        funTable = new RolapSchemaFunctionTable(userDefinedFunctions);
        ((RolapSchemaFunctionTable) funTable).init();
    }

    /**
     * <code>RolapStarRegistry</code> is a registry for {@link RolapStar}s.
     */
    public class RolapStarRegistry {
        private final Map<PhysRelation, RolapStar> stars =
            new HashMap<PhysRelation, RolapStar>();

        RolapStarRegistry() {
        }

        /**
         * Looks up a {@link RolapStar}, creating it if it does not exist.
         *
         * <p> {@link RolapSchemaUpgrader#addJoin} works in a similar way.
         */
        synchronized RolapStar getOrCreateStar(
            final PhysRelation fact)
        {
            RolapStar star = stars.get(fact);
            if (star == null) {
                star = makeRolapStar(fact);
                stars.put(fact, star);
            }
            return star;
        }

        synchronized RolapStar getStar(final String factTableName) {
            return stars.get(factTableName);
        }

        synchronized Collection<RolapStar> getStars() {
            return stars.values();
        }
    }

    private RolapStarRegistry rolapStarRegistry = new RolapStarRegistry();

    public RolapStarRegistry getRolapStarRegistry() {
        return rolapStarRegistry;
    }

    /**
     * Function table which contains all of the user-defined functions in this
     * schema, plus all of the standard functions.
     */
    static class RolapSchemaFunctionTable extends FunTableImpl {
        private final List<UdfResolver.UdfFactory> udfFactoryList;

        RolapSchemaFunctionTable(Collection<UdfResolver.UdfFactory> udfs) {
            udfFactoryList = new ArrayList<UdfResolver.UdfFactory>(udfs);
        }

        public void defineFunctions(Builder builder) {
            final FunTable globalFunTable = GlobalFunTable.instance();
            for (String reservedWord : globalFunTable.getReservedWords()) {
                builder.defineReserved(reservedWord);
            }
            for (Resolver resolver : globalFunTable.getResolvers()) {
                builder.define(resolver);
            }
            for (UdfResolver.UdfFactory udfFactory : udfFactoryList) {
                builder.define(new UdfResolver(udfFactory));
            }
        }
    }

    public RolapStar getStar(final String factTableName) {
        return getRolapStarRegistry().getStar(factTableName);
    }

    public Collection<RolapStar> getStars() {
        return getRolapStarRegistry().getStars();
    }

    final RolapNativeRegistry nativeRegistry = new RolapNativeRegistry();

    RolapNativeRegistry getNativeRegistry() {
        return nativeRegistry;
    }

    /**
     * @return Returns the dataSourceChangeListener.
     */
    public DataSourceChangeListener getDataSourceChangeListener() {
        return dataSourceChangeListener;
    }

    /**
     * @param dataSourceChangeListener The dataSourceChangeListener to set.
     */
    public void setDataSourceChangeListener(
        DataSourceChangeListener dataSourceChangeListener)
    {
        this.dataSourceChangeListener = dataSourceChangeListener;
    }

    /**
     * Location of a node in an XML document.
     */
    public interface XmlLocation {
        String getRange();

        /**
         * Returns a similar location, but specifying an attribute.
         *
         * @param attributeName Attribute name (may be null)
         * @return Location of attribute
         */
        XmlLocation at(String attributeName);
    }

    /**
     * Implementation of XmlLocation based on {@link Location}.
     */
    static class XmlLocationImpl implements XmlLocation {
        private final NodeDef node;
        private final Location location;
        private final String attributeName;

        /**
         * Creates an XmlLocationImpl.
         *
         * @param node XML node
         * @param location Location
         * @param attributeName Attribute name (may be null)
         */
        XmlLocationImpl(
            NodeDef node, Location location, String attributeName)
        {
            this.node = node;
            this.location = location;
            this.attributeName = attributeName;
        }

        public String toString() {
            return location == null ? "null" : location.toString();
        }

        public String getRange() {
            final int start = location.getStartPos();
            final int end = start + location.getText(true).length();
            return start + "-" + end;
        }

        public XmlLocation at(String attributeName) {
            if (Util.equals(attributeName, this.attributeName)) {
                return this;
            }
            return new XmlLocationImpl(node, location, attributeName);
        }
    }

    /**
     * Collection of relations and links (relationships) between them.
     *
     * <p>The directed graph formed by the relations and links is connected and
     * acyclic: there is a unique path from any relation to any other relation,
     * following a sequence of links in the appropriate direction.
     */
    public static class PhysSchema {
        // We use a linked hash map for determinacy; the order that tables are
        // declared doesn't matter (except that if there are duplicates, the
        // later one will be discarded).
        final LinkedHashMap<String, PhysRelation> tablesByName =
            new LinkedHashMap<String, PhysRelation>();
        final Dialect dialect;
        final JdbcSchema jdbcSchema;

        final Set<PhysLink> linkSet = new HashSet<PhysLink>();

        private final Map<PhysRelation, List<PhysLink>> hardLinksFrom =
            new HashMap<PhysRelation, List<PhysLink>>();

        private int nextAliasId = 0;

        private final PhysSchemaGraph schemaGraph =
            new PhysSchemaGraph(
                this, Collections.<RolapSchema.PhysLink>emptyList());

        private int columnCount;

        public final PhysStatistic statistic;

        /**
         * Creates a physical schema.
         *
         * @param dialect Dialect
         * @param internalConnection Internal connection (for data source, and
         *                           accounting of stats queries)
         */
        public PhysSchema(
            Dialect dialect,
            RolapConnection internalConnection)
        {
            this.dialect = dialect;
            this.jdbcSchema =
                JdbcSchema.makeDB(internalConnection.getDataSource());
            jdbcSchema.load();
            statistic = new PhysStatistic(dialect, internalConnection);
        }

        /**
         * Adds a link to this schema.
         *
         * @param sourceKey Key (usually primary) of source table
         * @param targetRelation Target table (contains foreign key)
         * @param columnList List of foreign key columns
         * @param hard Whether the link is hard; that is, whether a join over
         * the link should be generated every time the source of the link is
         * referenced
         * @return whether the link was added (per {@link Set#add(Object)})
         */
        public boolean addLink(
            PhysKey sourceKey,
            PhysRelation targetRelation,
            List<PhysColumn> columnList,
            boolean hard)
        {
            final PhysLink physLink =
                new PhysLink(sourceKey, targetRelation, columnList);
            if (hard) {
                List<PhysLink> list = hardLinksFrom.get(targetRelation);
                if (list == null) {
                    list = new ArrayList<PhysLink>(1);
                    hardLinksFrom.put(targetRelation, list);
                }
                list.add(physLink);
            }
            if (linkSet.add(physLink)) {
                this.schemaGraph.addLink(physLink);
                return true;
            } else {
                return false;
            }
        }

        public List<PhysLink> hardLinksFrom(PhysRelation from) {
            Util.deprecated("not used - remove", false);
            final List<PhysLink> linkList = hardLinksFrom.get(from);
            if (linkList == null) {
                return Collections.emptyList();
            } else {
                return linkList;
            }
        }

        /**
         * Generates a new alias, distinct from aliases of relations registered
         * in this schema and other aliases that have been generated by this
         * method for this schema.
         *
         * <p>There is no guarantee that a relation will not be subsequently
         * registered with a matching user-defined alias, but this is unlikely.
         *
         * @return Unique relation alias
         */
        public String newAlias() {
            while (true) {
                String alias = "_" + nextAliasId++;
                if (!tablesByName.containsKey(alias)) {
                    return alias;
                }
            }
        }

        /**
         * Returns the default graph for this schema. The graph contains all
         * relations and links.
         *
         * @return Default graph
         */
        public PhysSchemaGraph getGraph() {
            return schemaGraph;
        }

        public int getColumnCount() {
            return columnCount;
        }

        List<ColumnInfo> describe(
            RolapSchemaLoader loader,
            NodeDef xmlNode,
            String sql)
        {
            java.sql.Connection connection = null;
            PreparedStatement pstmt = null;
            try {
                connection =
                    jdbcSchema.getDataSource().getConnection();
                pstmt = connection.prepareStatement(sql);
                final ResultSetMetaData metaData = pstmt.getMetaData();
                final int columnCount = metaData.getColumnCount();
                final List<ColumnInfo> columnInfoList =
                    new ArrayList<ColumnInfo>();
                for (int i = 0; i < columnCount; i++) {
                    final String columnName =  metaData.getColumnName(i + 1);
                    final String typeName = metaData.getColumnTypeName(i + 1);
                    final int type = metaData.getColumnType(i + 1);
                    // REVIEW: We want the physical size of the column in bytes.
                    //  Ideally it would be comparable with the value returned
                    //  from DatabaseMetaData.getColumns for a base table.
                    final int columnSize = metaData.getColumnDisplaySize(i + 1);
                    assert columnSize > 0;
                    final Dialect.Datatype datatype =
                        dialect.sqlTypeToDatatype(typeName, type);
                    if (datatype == null) {
                        loader.getHandler().warning(
                            "Unknown data type "
                            + typeName + " (" + type + ") for column "
                            + columnName + " of view; mondrian is probably"
                            + " not familiar with this database's type"
                            + " system",
                            xmlNode,
                            null);
                        continue;
                    }
                    columnInfoList.add(
                        new ColumnInfo(columnName, datatype, columnSize));
                }
                pstmt.close();
                pstmt = null;
                connection.close();
                connection = null;
                return columnInfoList;
            } catch (SQLException e) {
                loader.getHandler().warning(
                    "View is invalid: " + e.getMessage() + "\nSQL: " + sql,
                    xmlNode, null, e);
                return null;
            } finally {
                //noinspection ThrowableResultOfMethodCallIgnored
                Util.close(null, pstmt, connection);
            }
        }
    }

    public static abstract class AttributeLink
        implements DirectedGraph.Edge<RolapAttribute>
    {
    }

    /**
     * A view onto a schema containing all of the nodes (relations), all of
     * the arcs (directed links between relations) and perhaps some extra arcs.
     */
    public static class AttributeGraph {

        private final DirectedGraph<RolapAttribute, AttributeLink> graph =
            new DirectedGraph<RolapAttribute, AttributeLink>();

        /**
         * Creates an AttributeGraph.
         */
        public AttributeGraph()
        {
        }
    }

    /**
     * A view onto a schema containing all of the nodes (relations), all of
     * the arcs (directed links between relations) and perhaps some extra arcs.
     */
    public static class PhysSchemaGraph {
        private final PhysSchema physSchema;

        private final DirectedGraph<PhysRelation, PhysLink> graph =
            new DirectedGraph<PhysRelation, PhysLink>();

        /**
         * Creates a PhysSchemaGraph.
         *
         * @param physSchema Schema
         * @param linkList Links of the graph; a subset of the links in the
         * schema
         */
        public PhysSchemaGraph(
            PhysSchema physSchema,
            Collection<PhysLink> linkList)
        {
            this.physSchema = physSchema;

            // Populate the graph. Check that every link connects a pair of
            // nodes in the schema, and is registered in the schema.
            for (PhysLink link : linkList) {
                addLink(link);
            }
        }

        /**
         * Adds a link to this graph. The link and nodes at the ends of the link
         * must belong to the same {@link mondrian.rolap.RolapSchema.PhysSchema}
         * as the graph. If the graph already contains this link, does not add
         * it again.
         *
         * @param link Link
         * @return Whether link was added
         */
        public boolean addLink(PhysLink link) {
            assert link.getFrom().getSchema() == physSchema;
            assert link.getTo().getSchema() == physSchema;
            assert physSchema.linkSet.contains(link);

            if (!graph.edgeList().contains(link)) {
                graph.addEdge(link);
                return true;
            } else {
                return false;
            }
        }

        /**
         * Adds to a list the hops necessary to go from one relation to another.
         *
         *
         * @param pathBuilder Path builder to which to add path
         * @param prevRelation Relation to start at
         * @param nextRelations Relation to jump to
         * @param directed Whether to treat graph as directed
         * @throws PhysSchemaException if there is not a unique path
         */
        private void addHopsBetween(
            PhysPathBuilder pathBuilder,
            PhysRelation prevRelation,
            Set<PhysRelation> nextRelations,
            boolean directed)
            throws PhysSchemaException
        {
            if (nextRelations.contains(prevRelation)) {
                return;
            }
            if (nextRelations.size() == 0) {
                throw new IllegalArgumentException("nextRelations is empty");
            }
            if (directed) {
                final List<PhysLink> path =
                    findUniquePath(prevRelation, nextRelations);
                for (PhysLink link : path) {
                    if (nextRelations.contains(link.targetRelation)) {
                        break;
                    }
                    pathBuilder.add(link, link.sourceKey.relation, true);
                }
            } else {
                List<Pair<PhysLink, Boolean>> path =
                    findUniquePathUndirected(prevRelation, nextRelations);
                for (Pair<PhysLink, Boolean> pair : path) {
                    final PhysLink link = pair.left;
                    final boolean forward = pair.right;
                    PhysRelation targetRelation =
                        forward
                            ? link.targetRelation
                            : link.sourceKey.relation;
                    PhysRelation sourceRelation =
                        forward
                            ? link.sourceKey.relation
                            : link.targetRelation;
                    if (nextRelations.contains(targetRelation)) {
                        break;
                    }
                    pathBuilder.add(link, sourceRelation, forward);
                }
            }
        }

        private List<PhysLink> findUniquePath(
            PhysRelation prevRelation,
            Set<PhysRelation> nextRelations)
            throws PhysSchemaException
        {
            for (PhysRelation nextRelation : nextRelations) {
                final List<List<PhysLink>> pathList =
                    graph.findAllPaths(prevRelation, nextRelation);
                switch (pathList.size()) {
                case 0:
                    continue;
                case 1:
                    return pathList.get(0);
                default:
                    throw new PhysSchemaException(
                        "Needed to find exactly one path from " + prevRelation
                        + " to " + nextRelation + ", but found "
                        + pathList.size() + " (" + pathList + ")");
                }
            }
            throw new PhysSchemaException(
                "Could not find a path from " + prevRelation
                + " to any of " + nextRelations);
        }

        private List<Pair<PhysLink, Boolean>> findUniquePathUndirected(
            PhysRelation prevRelation,
            Set<PhysRelation> nextRelations)
            throws PhysSchemaException
        {
            for (PhysRelation nextRelation : nextRelations) {
                List<List<Pair<PhysLink, Boolean>>> pathList =
                    graph.findAllPathsUndirected(prevRelation, nextRelation);
                switch (pathList.size()) {
                case 0:
                    continue;
                case 1:
                    return pathList.get(0);
                default:
                    // When more than one path is possible,
                    // we use the one with the least amount of joins.
                    List<Pair<PhysLink, Boolean>> smallest = null;
                    for (List<Pair<PhysLink, Boolean>> path : pathList) {
                        if (smallest == null
                            || smallest.size() > path.size())
                        {
                            smallest = path;
                        }
                    }
                    return smallest;
                }
            }
            throw new PhysSchemaException(
                "Could not find a path from " + prevRelation
                + " to any of " + nextRelations);
        }

        /**
         * Creates a path from one relation to another.
         *
         * @param relation Start relation
         * @param relation1 Relation to jump to
         * @return path, never null
         *
         * @throws PhysSchemaException if there is not a unique path
         */
        public PhysPath findPath(
            PhysRelation relation,
            PhysRelation relation1)
            throws PhysSchemaException
        {
            return findPath(relation, Collections.singleton(relation1), true);
        }

        /**
         * Creates a path from one relation to another.
         *
         *
         * @param relation Start relation
         * @param targetRelations Relation to jump to
         * @param directed Whether to treat graph as directed
         * @return path, never null
         *
         * @throws PhysSchemaException if there is not a unique path
         */
        public PhysPath findPath(
            PhysRelation relation,
            Set<PhysRelation> targetRelations,
            boolean directed)
            throws PhysSchemaException
        {
            final PhysPathBuilder pathBuilder = new PhysPathBuilder(relation);
            addHopsBetween(
                pathBuilder,
                relation,
                targetRelations,
                directed);
            return pathBuilder.done();
        }

        /**
         * Appends to a path builder a path from the last step of the path
         * in the path builder to the given relation.
         *
         * <p>If there is no such path, throws.
         *
         * @param pathBuilder Path builder
         * @param relation Relation to hop to
         * @throws mondrian.rolap.RolapSchema.PhysSchemaException If no path
         *   can be found
         */
        public void findPath(
            PhysPathBuilder pathBuilder,
            PhysRelation relation)
            throws PhysSchemaException
        {
            addHopsBetween(
                pathBuilder,
                pathBuilder.hopList.get(
                    pathBuilder.hopList.size() - 1).relation,
                Collections.<PhysRelation>singleton(relation), true);
        }
    }

    public interface PhysRelation {

        /**
         * Returns the column in this relation with a given name. If the column
         * is not found, throws an error (if {@code fail} is true) or returns
         * null (if {@code fail} is false).
         *
         * @param columnName Column name
         * @param fail Whether to fail if column is not found
         * @return Column, or null if column is not found and fail is false
         */
        PhysColumn getColumn(String columnName, boolean fail);

        String getAlias();

        PhysSchema getSchema();

        /**
         * Defines a key in this relation.
         *
         * @param keyName Name of the key; must not be null, by convention, the
         *     key is called "primary" if user did not explicitly name it
         * @param keyColumnList List of columns in the key. May be empty if
         *     the columns have not been resolved yet
         * @return Key
         */
        PhysKey addKey(String keyName, List<PhysColumn> keyColumnList);

        /**
         * Looks up a key by the constituent columns, optionally creating the
         * key if not found.
         *
         * <p>Returns null if the key is not found and {@code add} is false.
         *
         * @param physColumnList Key columns
         * @param add Whether to add if not found
         * @return Key, if found or created, otherwise null
         */
        PhysKey lookupKey(List<PhysColumn> physColumnList, boolean add);

        /**
         * Looks up a key by name.
         *
         * @param key Key name
         * @return Key, or null if not found
         */
        PhysKey lookupKey(String key);

        Collection<PhysKey> getKeyList();

        /**
         * Returns the volume of the table. (Number of rows multiplied by the
         * size of a row in bytes.)
         */
        int getVolume();

        /**
         * Returns the number of rows in the table.
         */
        int getRowCount();

        void addColumn(PhysColumn column);

        PhysRelation cloneWithAlias(String newAlias);
    }

    static abstract class PhysRelationImpl implements PhysRelation {
        final PhysSchema physSchema;
        final String alias;
        final LinkedHashMap<String, PhysColumn> columnsByName =
            new LinkedHashMap<String, PhysColumn>();
        final LinkedHashMap<String, PhysKey> keysByName =
            new LinkedHashMap<String, PhysKey>();
        private boolean populated;
        private int totalColumnByteCount;
        private int rowCount;

        PhysRelationImpl(
            PhysSchema physSchema,
            String alias)
        {
            this.alias = alias;
            this.physSchema = physSchema;
        }

        LinkedHashMap<String, PhysColumn> getColumnsByName() {
            return columnsByName;
        }

        void setPopulated(boolean populated) {
            this.populated = populated;
        }

        public abstract int hashCode();

        public abstract boolean equals(Object obj);

        public PhysSchema getSchema() {
            return physSchema;
        }

        void addAllColumns(PhysRelationImpl relation) {
            for (PhysColumn column : relation.getColumnsByName().values()) {
                addColumn(column.cloneWithAlias(this));
            }
        }

        void addAllKeys(PhysRelationImpl relation) {
            for (PhysKey physKey : relation.getKeyList()) {
                addKey(physKey.name, physKey.columnList);
            }
        }

        public PhysColumn getColumn(String columnName, boolean fail) {
            final PhysColumn column = columnsByName.get(columnName);
            if (column == null && fail) {
                throw Util.newError(
                    "Column '" + columnName + "' not found in relation '"
                    + this + "'");
            }
            return column;
        }

        public String getAlias() {
            return alias;
        }

        public Collection<PhysKey> getKeyList() {
            return keysByName.values();
        }

        public int getVolume() {
            return getTotalColumnSize() * getRowCount();
        }

        protected int getTotalColumnSize() {
            return totalColumnByteCount;
        }

        public int getRowCount() {
            return rowCount;
        }

        public PhysKey lookupKey(
            List<PhysColumn> physColumnList, boolean add)
        {
            for (PhysKey key : keysByName.values()) {
                if (key.columnList.equals(physColumnList)) {
                    return key;
                }
            }
            if (add) {
                // generate a name of the key, unique within the table
                int i = keysByName.size();
                String keyName;
                for (;;) {
                    keyName = "key$" + i;
                    if (!keysByName.containsKey(keyName)) {
                        break;
                    }
                    ++i;
                }
                return addKey(keyName, physColumnList);
            }
            return null;
        }

        public PhysKey lookupKey(String keyName) {
            return keysByName.get(keyName);
        }

        public PhysKey addKey(String keyName, List<PhysColumn> keyColumnList) {
            final PhysKey key = new PhysKey(this, keyName, keyColumnList);
            final PhysKey previous = keysByName.put(keyName, key);
            if (previous != null) {
                // OK if the table already has a key, as long as it is
                // identical.
                assert previous.equals(key);
                keysByName.put(keyName, previous);
                return previous;
            }
            return key;
        }

        /**
         * Loads this table's column definitions from the schema, if that has
         * not been done already. Returns whether the columns were successfully
         * populated this time or previously.
         *
         * <p>If the table does not exist or the view is invalid, returns false,
         * and calls {@link RolapSchemaLoader.Handler#warning} to indicate
         * the problem.
         *
         * @param loader Schema loader
         * @param xmlNode XML element
         * @return whether was populated successfully this call or previously
         */
        public boolean ensurePopulated(
            RolapSchemaLoader loader,
            NodeDef xmlNode)
        {
            if (!populated) {
                final int[] rowCountAndSize = new int[2];
                populated = populateColumns(loader, xmlNode, rowCountAndSize);
                rowCount = rowCountAndSize[0];
                totalColumnByteCount = rowCountAndSize[1];
            }
            return populated;
        }

        /**
         * Populates the columns of a table by querying JDBC metadata.
         *
         * <p>Returns whether populated successfully. If there was an error (say
         * if table was not found or view had an error), posts a warning and
         * returns false.
         *
         * @return Whether table was found
         * @param loader Schema (for logging errors)
         * @param xmlNode XML element
         * @param rowCountAndSize Output array, to hold the number of rows in
         */
        protected abstract boolean populateColumns(
            RolapSchemaLoader loader,
            NodeDef xmlNode,
            int[] rowCountAndSize);

        public void addColumn(PhysColumn column) {
            columnsByName.put(
                column.name,
                column);
        }
    }

    /**
     * A relation defined by a SQL string.
     *
     * <p>Column names and types are resolved, in
     * {@link mondrian.rolap.RolapSchema.PhysRelationImpl#populateColumns(RolapSchemaLoader, org.eigenbase.xom.NodeDef, int[])},
     * by preparing a query based on the SQL string.
     *
     * <p>In Mondrian's schema file, each {@link Dialect} can have its own SQL
     * string, but here we already know which dialect we are dealing with, so
     * there is a single SQL string.
     */
    public static class PhysView
        extends PhysRelationImpl
        implements PhysRelation
    {
        private final String sqlString;

        /**
         * Creates a view.
         *
         * @param physSchema Schema
         * @param alias Alias
         * @param sqlString SQL string
         */
        PhysView(
            PhysSchema physSchema,
            String alias,
            String sqlString)
        {
            super(physSchema, alias);
            this.sqlString = sqlString;
            assert sqlString != null && sqlString.length() > 0 : sqlString;
        }

        public PhysRelation cloneWithAlias(String newAlias) {
            PhysView physView = new PhysView(physSchema, newAlias, sqlString);
            physView.addAllColumns(this);
            physView.addAllKeys(this);
            physView.setPopulated(true);
            return physView;
        }

        /**
         * Returns the SQL query that defines this view in the current dialect.
         *
         * @return SQL query
         */
        public String getSqlString() {
            return sqlString;
        }

        public int hashCode() {
            return Util.hashV(0, physSchema, alias, sqlString);
        }

        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj instanceof PhysView) {
                PhysView that = (PhysView) obj;
                return this.alias.equals(that.alias)
                    && this.sqlString.equals(that.sqlString)
                    && this.physSchema.equals(that.physSchema);
            }
            return false;
        }

        protected boolean populateColumns(
            RolapSchemaLoader loader,
            NodeDef xmlNode,
            int[] rowCountAndSize)
        {
            final List<ColumnInfo> columnInfoList =
                physSchema.describe(loader, xmlNode, sqlString);
            if (columnInfoList == null) {
                return false;
            }
            for (ColumnInfo columnInfo : columnInfoList) {
                addColumn(
                    new RolapSchema.PhysRealColumn(
                        this,
                        columnInfo.name,
                        columnInfo.datatype,
                        null,
                        columnInfo.size));
            }
            final int rowCount = 1; // TODO:
            int rowByteCount = 0;
            for (PhysColumn physColumn : columnsByName.values()) {
                rowByteCount += physColumn.getColumnSize();
            }
            rowCountAndSize[0] = rowCount;
            rowCountAndSize[1] = rowByteCount;
            return true;
        }
    }

    static class ColumnInfo {
        String name;
        Dialect.Datatype datatype;
        int size;

        public ColumnInfo(String name, Dialect.Datatype datatype, int size) {
            this.name = name;
            this.datatype = datatype;
            this.size = size;
        }
    }

    /**
     * Relation defined by a fixed set of explicit row values. The number of
     * rows is generally small.
     */
    public static class PhysInlineTable
        extends PhysRelationImpl
        implements PhysRelation
    {
        final List<String[]> rowList = new ArrayList<String[]>();

        /**
         * Creates an inline table.
         *
         * @param physSchema Schema
         * @param alias Name of inline table within schema
         */
        PhysInlineTable(
            PhysSchema physSchema,
            String alias)
        {
            super(physSchema, alias);
            assert alias != null;
        }

        public PhysRelation cloneWithAlias(String newAlias) {
            PhysInlineTable physInlineTable =
                new PhysInlineTable(this.physSchema, newAlias);
            physInlineTable.addAllColumns(this);
            physInlineTable.addAllKeys(this);
            physInlineTable.setPopulated(true);
            physInlineTable.setRowList(rowList);
            return physInlineTable;
        }

        private void setRowList(List<String[]> rowList) {
            this.rowList.addAll(rowList);
        }

        @Override
        public String toString() {
            return alias;
        }

        public int hashCode() {
            return Util.hashV(0, physSchema, alias);
        }

        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj instanceof PhysInlineTable) {
                PhysInlineTable that = (PhysInlineTable) obj;
                return this.alias.equals(that.alias)
                    && this.physSchema.equals(that.physSchema);
            }
            return false;
        }

        protected boolean populateColumns(
            RolapSchemaLoader loader, NodeDef xmlNode, int[] rowCountAndSize)
        {
            // not much to do; was populated on creation
            rowCountAndSize[0] = rowList.size();
            rowCountAndSize[1] = columnsByName.size() * 4;
            return true;
        }
    }

    public static class PhysTable extends PhysRelationImpl
    {
        final String schemaName;
        final String name;
        private Map<String, String> hintMap;
        private int rowCount;
        private Hook hook;

        /**
         * Creates a table.
         *
         * <p>Does not populate the column definitions from JDBC; see
         * {@link mondrian.rolap.RolapSchema.PhysRelationImpl#ensurePopulated}
         * for that.
         *
         * <p>The {@code hintMap} parameter is a map from hint type to hint
         * text. It is never null, but frequently empty. It is treated as
         * immutable; the constructor does not clone the collection. How to
         * generate hints into SQL is up to the {@link Dialect}.
         *
         * @param physSchema Schema
         * @param schemaName Schema name
         * @param name Table name
         * @param alias Table alias that identifies this use of the table, must
         *     be unique within relations in this schema
         * @param hintMap Map from hint type to hint text
         */
        public PhysTable(
            PhysSchema physSchema,
            String schemaName,
            String name,
            String alias,
            Map<String, String> hintMap)
        {
            super(physSchema, alias);
            this.schemaName = schemaName;
            this.name = name;
            this.hintMap = hintMap;
            assert name != null;
            assert alias != null;
        }

        public PhysRelation cloneWithAlias(String newAlias) {
            PhysTable physTable = new PhysTable(
                physSchema, schemaName, name, newAlias, hintMap);
            physTable.addAllColumns(this);
            physTable.addAllKeys(this);
            physTable.setPopulated(true);
            physTable.setRowCount(this.getRowCount());
            return physTable;
        }

        private void setRowCount(int rowCount) {
            this.rowCount = rowCount;
        }

        public String toString() {
            return (schemaName == null ? "" : (schemaName + '.'))
                + name
                + (name.equals(alias) ? "" : (" as " + alias));
        }

        public int hashCode() {
            return Util.hashV(
                0, physSchema, schemaName, name, alias);
        }

        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj instanceof PhysTable) {
                PhysTable that = (PhysTable) obj;
                return alias.equals(that.alias)
                    && name.equals(that.name)
                    && schemaName.equals(that.schemaName)
                    && physSchema.equals(that.physSchema);
            }
            return false;
        }

        /**
         * Returns the name of the database schema this table resides in.
         *
         * @return name of database schema, may be null
         */
        public String getSchemaName() {
            return schemaName;
        }

        /**
         * Returns the name of the table in the database.
         *
         * @return table name
         */
        public String getName() {
            return name;
        }

        protected boolean populateColumns(
            RolapSchemaLoader loader, NodeDef xmlNode, int[] rowCountAndSize)
        {
            JdbcSchema.Table jdbcTable =
                physSchema.jdbcSchema.getTable(name);
            if (jdbcTable == null) {
                if (hook == null) {
                    loader.getHandler().warning(
                        "Table '" + name + "' does not exist in database.",
                        xmlNode,
                        null);
                    return false;
                }
                hook.apply(
                    this,
                    loader.schema.getInternalConnection());
                hook = null;
                try {
                    jdbcTable = physSchema.jdbcSchema.reloadTable(name);
                } catch (SQLException e) {
                    throw Util.newError(
                        "Error while re-loading table '" + name + "'");
                }
            }
            try {
                jdbcTable.load();
            } catch (SQLException e) {
                throw Util.newError(
                    "Error while loading columns of table '" + name + "'");
            }

            rowCount =
                physSchema.statistic.getRelationCardinality(
                    this,
                    alias,
                    -1);

            for (JdbcSchema.Table.Column jdbcColumn : jdbcTable.getColumns()) {
                PhysColumn column =
                    columnsByName.get(
                        jdbcColumn.getName());
                if (column == null) {
                    column =
                        new PhysRealColumn(
                            this,
                            jdbcColumn.getName(),
                            jdbcColumn.getDatatype(),
                            null,
                            jdbcColumn.getColumnSize());
                    addColumn(column);
                }
            }
            return true;
        }

        public int getRowCount() {
            return rowCount;
        }

        public Map<String, String> getHintMap() {
            return hintMap;
        }

        public void setHook(Hook hook) {
            this.hook = hook;
        }

        interface Hook {
            boolean apply(
                PhysTable table,
                RolapConnection connection);
        }
    }

    /**
     * A column (later, perhaps a collection of columns) that identifies a
     * record in a table. The main purpose is as a target for a
     * {@link mondrian.rolap.RolapSchema.PhysLink}.
     *
     * <p>Unlike a primary or unique key in a database, a PhysKey is
     * not necessarily unique. For instance, the time dimension table may have
     * one record per day, but a particular fact table may link at the month
     * level. This is fine because Mondrian automatically eliminates duplicates
     * when reading any level.
     *
     * <p>REVIEW: Should one of the keys be flagged as the 'main' key of a
     * relation? Should keys be flagged as 'unique'?
     */
    public static class PhysKey {
        final PhysRelation relation;
        final List<PhysColumn> columnList;
        final String name;

        /**
         * Creates a PhysKey.
         *
         * @param relation Relation that the key belongs to
         * @param name Name of key
         * @param columnList List of columns
         */
        public PhysKey(
            PhysRelation relation,
            String name,
            List<PhysColumn> columnList)
        {
            assert relation != null;
            assert name != null;
            assert columnList != null;
            this.relation = relation;
            this.name = name;
            this.columnList = columnList;
        }

        @Override
        public int hashCode() {
            return Util.hashV(
                0,
                relation,
                columnList);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof PhysKey) {
                final PhysKey that = (PhysKey) obj;
                return this.relation.equals(that.relation)
                    && this.columnList.equals(that.columnList);
            }
            return false;
        }

        public String toString() {
            return "[Key " + relation + " (" + columnList + ")]";
        }
    }

    /**
     * Link between two tables, also known as a relationship.
     *
     * <p>A link has a direction: it is said to be from the target table (with
     * the foreign key) to the source table (which contains the primary key).
     * It is in the 'many to one' direction.
     *
     * <p>For example, in the FoodMart star schema, there are links from the
     * fact table SALES_FACT to the dimension tables TIME_BY_DAY and PRODUCT,
     * and a further link from PRODUCT to PRODUCT_CLASS, making Product a
     * snowflake dimension.
     */
    public static class PhysLink implements DirectedGraph.Edge<PhysRelation> {
        final PhysKey sourceKey;
        final PhysRelation targetRelation;
        final List<PhysColumn> columnList;
        final String sql;

        /**
         * Creates a link from {@code targetTable} to {@code sourceTable} over
         * a list of columns.
         *
         * @param sourceKey Key of source table (usually the primary key)
         * @param targetRelation Target table (contains foreign key)
         * @param columnList Foreign key columns in target table
         */
        public PhysLink(
            PhysKey sourceKey,
            PhysRelation targetRelation,
            List<PhysColumn> columnList)
        {
            this.sourceKey = sourceKey;
            this.targetRelation = targetRelation;
            this.columnList = columnList;
            assert columnList.size() == sourceKey.columnList.size()
                : columnList + " vs. " + sourceKey.columnList;
            for (PhysColumn column : columnList) {
                assert column.relation == targetRelation
                    : column.relation + "/" + targetRelation;
            }
            this.sql = deriveSql();
        }

        public int hashCode() {
            return Util.hashV(0, sourceKey, targetRelation, columnList);
        }

        public boolean equals(Object obj) {
            if (obj instanceof PhysLink) {
                PhysLink that = (PhysLink) obj;
                return this.sourceKey.equals(that.sourceKey)
                    && this.targetRelation.equals(that.targetRelation)
                    && this.columnList.equals(that.columnList);
            }
            return false;
        }

        public String toString() {
            return "Link from " + targetRelation + " "
                + columnList
                + " to " + sourceKey;
        }

        public PhysRelation getFrom() {
            return targetRelation;
        }

        public PhysRelation getTo() {
            return sourceKey.relation;
        }

        public String toSql() {
            return sql;
        }

        private String deriveSql() {
            final StringBuilder buf = new StringBuilder();
            for (int i = 0; i < columnList.size(); i++) {
                if (buf.length() > 0) {
                    buf.append(" and ");
                }
                PhysColumn targetColumn = columnList.get(i);
                final PhysExpr sourceColumn = sourceKey.columnList.get(i);
                buf.append(targetColumn.toSql())
                    .append(" = ").append(sourceColumn.toSql());
            }
            return buf.toString();
        }
    }

    public static abstract class PhysExpr {
        /**
         * Returns the SQL string for this expression.
         *
         * <p>The expression is used as a key for cell requests, so this method
         * must be fast. Preferably return a pre-computed result.</p>
         */
        public abstract String toSql();

        /**
         * Calls a callback for each embedded PhysColumn.
         */
        public abstract void foreachColumn(
            Util.Function1<PhysColumn, Void> callback);

        /**
         * Returns the data type of this expression, or null if not known.
         *
         * @return Data type
         */
        public abstract Dialect.Datatype getDatatype();

        /**
         * Returns the type to be used for in-memory representation of this
         * expression.
         *
         * @return Internal type
         */
        public abstract SqlStatement.Type getInternalType();

        /**
         * Joins the table underlying this expression to the root of the
         * corresponding star. Usually called after
         * {@link SqlQueryBuilder#addToFrom(mondrian.rolap.RolapSchema.PhysExpr)}.
         *
         * @see Util#deprecated(Object, boolean) Any query-building code calling
         * this method should instead use the root attribute of the hierarchy
         * as a 'starting point'. Then the query will automatically join.
         *
         * @param sqlQuery Query whose FROM clause to add to
         * @param measureGroup If null, just add this expression's table; if
         *    not null, add a join path to the measure group's fact table
         * @param cubeDimension Dimension by which expression is joined to
         *    fact table. Must be specified if and only if measure group is
         *    specified.
         */
        public final void joinToStarRoot(
            final SqlQuery sqlQuery,
            final RolapMeasureGroup measureGroup,
            final RolapCubeDimension cubeDimension)
        {
            assert measureGroup != null;
            assert cubeDimension != null;
            foreachColumn(
                new Util.Function1<PhysColumn, Void>() {
                    public Void apply(PhysColumn column) {
                        final RolapStar.Column starColumn =
                            measureGroup.getRolapStarColumn(
                                cubeDimension, column, true);
                        starColumn.getTable().addToFrom(sqlQuery, false, true);
                        return null;
                    }
                }
            );
        }
    }

    public static class PhysTextExpr extends PhysExpr {
        private final String text;

        PhysTextExpr(String s) {
            this.text = s;
        }

        public String toSql() {
            return text;
        }

        public void addToFrom(
            SqlQueryBuilder queryBuilder)
        {
            // nothing to do
        }

        public void foreachColumn(Util.Function1<PhysColumn, Void> callback) {
            // nothing
        }

        public Dialect.Datatype getDatatype() {
            return null; // not known
        }

        public SqlStatement.Type getInternalType() {
            return null; // not known
        }
    }

    public static abstract class PhysColumn extends PhysExpr {
        public final PhysRelation relation;
        public final String name;
        Dialect.Datatype datatype; // may be null, temporarily
        protected final int columnSize;
        private final int ordinal;
        SqlStatement.Type internalType; // may be null

        public PhysColumn(
            PhysRelation relation,
            String name,
            int columnSize,
            Dialect.Datatype datatype,
            SqlStatement.Type internalType)
        {
            assert relation != null;
            assert name != null;
            this.name = name;
            this.relation = relation;
            this.columnSize = columnSize;
            this.datatype = datatype;
            this.internalType = internalType;
            this.ordinal = relation.getSchema().columnCount++;
        }

        public String toString() {
            return toSql();
        }

        public void setDatatype(Dialect.Datatype datatype) {
            this.datatype = datatype;
        }

        public Dialect.Datatype getDatatype() {
            return datatype;
        }

        public SqlStatement.Type getInternalType() {
            return internalType;
        }

        public void foreachColumn(Util.Function1<PhysColumn, Void> callback) {
            callback.apply(this);
        }

        /**
         * Returns the size in bytes of the column in the database.
         */
        public int getColumnSize() {
            return columnSize;
        }

        /**
         * Ordinal of column; non-negative, and unique within its schema.
         *
         * @return Ordinal of column.
         */
        public final int ordinal() {
            return ordinal;
        }

        public void setInternalType(SqlStatement.Type internalType) {
            this.internalType = internalType;
        }

        PhysColumn cloneWithAlias(PhysRelation newRelation) {
            return this;
        }
    }

    public static final class PhysRealColumn extends PhysColumn {
        private final String sql;

        PhysRealColumn(
            PhysRelation relation,
            String name,
            Dialect.Datatype datatype,
            SqlStatement.Type internalType,
            int columnSize)
        {
            super(relation, name, columnSize, datatype, internalType);
            this.sql = deriveSql();
        }

        @Override
        PhysColumn cloneWithAlias(PhysRelation newRelation) {
            return new PhysRealColumn(
                newRelation, name, datatype, internalType, columnSize);
        }

        protected String deriveSql() {
            return relation.getSchema().dialect.quoteIdentifier(
                relation.getAlias())
                + '.'
                + relation.getSchema().dialect.quoteIdentifier(name);
        }

        public int hashCode() {
            return Util.hash(name.hashCode(), relation);
        }

        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj instanceof PhysRealColumn) {
                PhysRealColumn that = (PhysRealColumn) obj;
                return name.equals(that.name)
                    && relation.equals(that.relation);
            }
            return false;
        }

        public String toSql() {
            return sql;
        }
    }

    public static final class PhysCalcColumn extends PhysColumn {
        private RolapSchemaLoader loader; // cleared once compute succeeds
        private NodeDef xmlNode; // cleared once compute succeeds
        final List<RolapSchema.PhysExpr> list;
        private String sql;

        PhysCalcColumn(
            RolapSchemaLoader loader,
            NodeDef xmlNode,
            PhysRelation table,
            String name,
            Dialect.Datatype datatype,
            SqlStatement.Type internalType,
            List<PhysExpr> list)
        {
            super(table, name, 4, datatype, internalType);
            this.loader = loader;
            this.xmlNode = xmlNode;
            this.list = list;
            compute();
        }

        public void compute() {
            if (loader != null
                && !list.isEmpty()
                && getUnresolvedColumnCount() == 0)
            {
                sql = deriveSql();
                if (datatype == null) {
                    final PhysSchema physSchema = relation.getSchema();
                    final SqlQuery query = new SqlQuery(physSchema.dialect);
                    query.addSelect(sql, null);
                    query.addFrom(relation, relation.getAlias(), true);
                    final List<ColumnInfo> columnInfoList =
                        physSchema.describe(loader, xmlNode, query.toSql());
                    if (columnInfoList != null
                        && columnInfoList.size() == 1)
                    {
                        datatype = columnInfoList.get(0).datatype;
                    }
                }
                loader = null;
                xmlNode = null;
            }
        }

        private int getUnresolvedColumnCount() {
            int unresolvedCount = 0;
            for (PhysExpr expr : list) {
                if (expr instanceof UnresolvedColumn) {
                    ++unresolvedCount;
                }
            }
            return unresolvedCount;
        }

        public String toSql() {
            return sql;
        }

        protected String deriveSql() {
            final StringBuilder buf = new StringBuilder();
            for (PhysExpr expr : list) {
                buf.append(expr.toSql());
            }
            return buf.toString();
        }

        @Override
        public void foreachColumn(Util.Function1<PhysColumn, Void> callback) {
            for (PhysExpr physExpr : list) {
                physExpr.foreachColumn(callback);
            }
        }

        public List<PhysExpr> getList() {
            return list;
        }
    }

    /** experimental - alternative to {@link mondrian.rolap.RolapSchema.PhysCalcColumn} */
    public static final class PhysCalcExpr extends PhysExpr {
        final List<RolapSchema.PhysExpr> list;
        private final String sql;

        PhysCalcExpr(
            List<RolapSchema.PhysExpr> list)
        {
            assert list != null;
            this.list = list;
            this.sql = deriveSql();
        }

        public Dialect.Datatype getDatatype() {
            return null;
        }

        public SqlStatement.Type getInternalType() {
            return null;
        }

        public String toSql() {
            return sql;
        }

        protected String deriveSql() {
            final StringBuilder buf = new StringBuilder();
            for (PhysExpr o : list) {
                buf.append(o.toSql());
            }
            return buf.toString();
        }

        public void foreachColumn(Util.Function1<PhysColumn, Void> callback) {
            for (Object o : list) {
                if (o instanceof PhysExpr) {
                    ((PhysExpr) o).foreachColumn(callback);
                }
            }
        }
    }

    public static abstract class UnresolvedColumn extends PhysColumn {
        State state = State.UNRESOLVED;
        final String tableName;
        final String name;
        final ElementDef xml;

        public UnresolvedColumn(
            PhysRelation relation,
            String tableName,
            String name,
            ElementDef xml)
        {
            // Boolean datatype is a dummy value, to keep an assert happy.
            super(relation, name, 0, Dialect.Datatype.Boolean, null);
            assert tableName != null;
            assert name != null;
            this.tableName = tableName;
            this.name = name;
            this.xml = xml;
        }

        public abstract void onResolve(PhysColumn column);

        public abstract String getContext();

        public String toString() {
            return tableName + "." + name;
        }

        public String toSql() {
            throw new UnsupportedOperationException(
                "unresolved column " + this);
        }

        public enum State {
            UNRESOLVED,
            ACTIVE,
            RESOLVED,
            ERROR
        }
    }

    public static class PhysHop {
        public final PhysRelation relation;
        public final PhysLink link;
        public final boolean forward;

        /**
         * Creates a hop.
         *
         * @param relation Target relation
         * @param link Link from source to target relation
         * @param forward Whether hop is in the default direction of the link
         */
        public PhysHop(
            PhysRelation relation,
            PhysLink link,
            boolean forward)
        {
            assert relation != null;
            // link is null for the first hop in a path
            this.relation = relation;
            this.link = link;
            this.forward = forward;
        }

        public boolean equals(Object obj) {
            return obj instanceof PhysHop
                && relation.equals(((PhysHop) obj).relation)
                && Util.equals(link, ((PhysHop) obj).link)
                && forward == ((PhysHop) obj).forward;
        }

        public int hashCode() {
            return Util.hashV(0, relation, link, forward);
        }

        public final PhysRelation fromRelation() {
            return forward
                ? link.sourceKey.relation
                : link.targetRelation;
        }

        public final PhysRelation toRelation() {
            return forward
                ? link.targetRelation
                : link.sourceKey.relation;
        }
    }

    /**
     * A path is a sequence of {@link PhysHop hops}.
     *
     * <p>It connects a pair of {@link PhysRelation relations} with a sequence
     * of link traversals. In general, a path between relations R<sub>1</sub>
     * and R<sub>n</sub> consists of the following hops:</p>
     *
     * <pre>
     *    { Hop(R<sub>1</sub>, null),
     *      Hop(R<sub>2</sub>, Link(R<sub>1</sub>, R<sub>2</sub>)),
     *      Hop(R<sub>3</sub>, Link(R<sub>2</sub>, R<sub>3</sub>)),
     *      ...
     *      Hop(R<sub>n</sub>, Link(R<sub>n-1</sub>, R<sub>n</sub>)) }
     * </pre>
     *
     * <p>Paths are immutable. The best way to create them is to uSe a
     * {@link PhysPathBuilder}.</p>
     *
     * <p>REVIEW: Is it worth making paths canonical? That is, if two paths
     * within a schema are equal, then they will always be the same object.</p>
     */
    public static class PhysPath {
        final List<PhysHop> hopList;

        public static final PhysPath EMPTY =
            new PhysPath(Collections.<PhysHop>emptyList());

        /**
         * Creates a path.
         *
         * @param hopList List of hops
         */
        public PhysPath(List<PhysHop> hopList) {
            this.hopList = hopList;
            for (int i = 0; i < hopList.size(); i++) {
                PhysHop hop = hopList.get(i);
                if (i == 0) {
                    assert hop.link == null;
                } else {
                    assert hop.relation == hop.fromRelation();
                    assert hopList.get(i - 1).relation == hop.toRelation();
                }
            }
        }

        /**
         * Returns list of links.
         *
         * @return list of links
         */
        public List<PhysLink> getLinks() {
            return new AbstractList<PhysLink>() {
                public PhysLink get(int index) {
                    return hopList.get(index + 1).link;
                }

                public int size() {
                    return hopList.size() - 1;
                }
            };
        }

        /**
         * Adds the relations in this path to the FROM clause of a query.
         *
         * @param query Query to add to
         * @param failIfExists Pass in false if you might have already added
         *     the table before and if that happens you want to do nothing.
         */
        public final void addToFrom(
            SqlQuery query,
            boolean failIfExists)
        {
            for (PhysHop physHop : hopList) {
                final PhysRelation relation = physHop.relation;
                query.addFrom(relation, relation.getAlias(), failIfExists);
            }
            // Add join conditions in reverse order so tests don't break - no
            // other reason - remove when everything works. Note that we stop
            // at 1, because hop 0 has no link.
            for (int i = hopList.size() - 1; i > 0; --i) {
                PhysHop physHop = hopList.get(i);
                final PhysRelation relation = physHop.relation;
                query.addFrom(relation, relation.getAlias(), failIfExists);
                query.addWhere(physHop.link.toSql());
            }
        }
    }

    public static class PhysPathBuilder {
        private final List<PhysHop> hopList = new ArrayList<PhysHop>();

        private PhysPathBuilder() {
        }

        PhysPathBuilder(PhysRelation relation) {
            this();
            hopList.add(new PhysHop(relation, null, true));
        }

        PhysPathBuilder(PhysPath path) {
            this();
            hopList.addAll(path.hopList);
        }

        public PhysPathBuilder add(
            PhysKey sourceKey,
            List<PhysColumn> columnList)
        {
            final PhysHop prevHop = hopList.get(hopList.size() - 1);
            add(
                new PhysLink(sourceKey, prevHop.relation, columnList),
                sourceKey.relation,
                true);
            return this;
        }

        public PhysPathBuilder add(
            PhysLink link,
            PhysRelation relation,
            boolean forward)
        {
            return add(new PhysHop(relation, link, forward));
        }

        public PhysPathBuilder add(PhysHop hop)
        {
            hopList.add(hop);
            return this;
        }

        public PhysPath done() {
            return new PhysPath(UnmodifiableArrayList.of(hopList));
        }

        @SuppressWarnings({
            "CloneDoesntCallSuperClone",
            "CloneDoesntDeclareCloneNotSupportedException"
        })
        public PhysPathBuilder clone() {
            final PhysPathBuilder pathBuilder = new PhysPathBuilder();
            pathBuilder.hopList.addAll(hopList);
            return pathBuilder;
        }
    }

    public interface PhysHint {
    }

    public class PhysHintImpl implements PhysHint {
    }

    /**
     * Callback that can give an expression a path to a root relation.
     *
     * <p>The most typical implementation is the one that knows the measure
     * group and joining dimension, and can therefore find the path of an
     * expression (say an attribute's key) to the fact table.</p>
     *
     * <p>If the {@link #path} method returns false, that means that the column
     * does not need to be joined to the fact table.</p>
     */
    public interface PhysRouter {
        /**
         * Returns the path by which the column should be joined to the fact
         * table.
         *
         * <p>A {@code null} return value is not an error; it means that the
         * column does not need to be joined.</p>
         *
         * @param column Column
         * @return Path by which column should be joined, or null
         */
        PhysPath path(PhysColumn column);
    }

    /**
     * Implementation of {@link PhysRouter} that always throws.
     *
     * <p>Use it in situations where a column will never need to be joined to
     * a fact table, for instance when defining a column predicate whose value
     * is a literal true or false.
     */
    public static class BadRouter implements RolapSchema.PhysRouter {
        public static final BadRouter INSTANCE = new BadRouter();

        private BadRouter() {
        }

        public RolapSchema.PhysPath path(RolapSchema.PhysColumn column) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Implementation of {@link PhysRouter} that always returns null paths.
     *
     * <p>Use it when a column does not need to be constrained by a fact table.
     * For example, if you want all products, not just products that have
     * been purchased.
     */
    public static class NoRouter implements RolapSchema.PhysRouter {
        public static final NoRouter INSTANCE = new NoRouter();

        private NoRouter() {
        }

        public RolapSchema.PhysPath path(RolapSchema.PhysColumn column) {
            return null;
        }
    }

    /**
     * Implementation of {@link PhysRouter} that joins an attribute to a
     * measure group's fact table via a joining dimension.
     */
    public static class CubeRouter implements PhysRouter {
        private final RolapMeasureGroup measureGroup;
        private final RolapCubeDimension cubeDimension;

        /**
         * Creates a cube router.
         *
         * @param measureGroup Measure group
         * @param cubeDimension Joining dimension
         */
        public CubeRouter(
            RolapMeasureGroup measureGroup,
            RolapCubeDimension cubeDimension)
        {
            this.measureGroup = measureGroup;
            this.cubeDimension = cubeDimension;
        }

        @Override
        public String toString() {
            return "CubeRouter(cube=" + measureGroup.getCube().getName()
                   + ", measureGroup=" + measureGroup.getName()
                   + ", dimension=" + cubeDimension.getName() + ")";
        }

        public PhysPath path(PhysColumn column) {
            return measureGroup.getPath(cubeDimension, column);
        }
    }

    /**
     * Checked exception for signaling errors in physical schemas.
     * These are intended to be caught and converted into validation exceptions.
     */
    static class PhysSchemaException extends Exception {
        /**
         * Creates a PhysSchemaException.
         *
         * @param message Message
         */
        public PhysSchemaException(String message) {
            super(message);
        }
    }

    public static class MondrianSchemaException extends RuntimeException {
        private final XmlLocation xmlLocation;

        public MondrianSchemaException(
            String message,
            String nodeDesc,
            XmlLocation xmlLocation,
            Severity severity,
            Throwable cause)
        {
            super(
                message
                + (nodeDesc == null
                    ? ""
                    : " (in " + nodeDesc + ")")
                + (xmlLocation == null
                    ? ""
                    : " (at " + xmlLocation + ")"),
                cause);
            this.xmlLocation = xmlLocation;
        }

        /**
         * Returns the location of the XML element or attribute that this
         * exception relates to, or null if not knnown.
         *
         * @return location of element or attribute
         */
        public XmlLocation getXmlLocation() {
            return xmlLocation;
        }
    }

    static enum Severity {
        WARNING,
        ERROR,
        FATAL
    }

    static class UnresolvedCalcColumn extends UnresolvedColumn {
        private final PhysCalcColumn physCalcColumn;
        private final List<PhysExpr> list;
        private final int index;

        /**
         * Creates an unresolved column reference.
         *
         * @param physTable Table that column belongs to
         * @param tableName Name of table
         * @param columnRef Column definition
         * @param sql SQL
         * @param list List of expressions
         * @param index Index within parent table
         */
        public UnresolvedCalcColumn(
            PhysTable physTable,
            String tableName,
            MondrianDef.Column columnRef,
            MondrianDef.SQL sql,
            PhysCalcColumn physCalcColumn,
            List<PhysExpr> list,
            int index)
        {
            super(physTable, tableName, columnRef.name, sql);
            this.physCalcColumn = physCalcColumn;
            this.list = list;
            this.index = index;
        }

        public String getContext() {
            return ", in definition of calculated column '"
                + physCalcColumn.relation.getAlias() + "'.'"
                + physCalcColumn.name + "'";
        }

        public void onResolve(
            PhysColumn column)
        {
            list.set(index, column);
            physCalcColumn.compute();
        }
    }

    public static class SqlQueryBuilder {
        public final SqlQuery sqlQuery;
        public final SqlTupleReader.ColumnLayoutBuilder layoutBuilder;
        private final Set<PhysRelation> relations =
            new LinkedHashSet<PhysRelation>();
        private final BitSet orderBitset = new BitSet();

        /**
         * Creates a SqlQueryBuilder.
         *
         * @param sqlQuery SQL query
         * @param layoutBuilder Column layout builder
         */
        public SqlQueryBuilder(
            SqlQuery sqlQuery,
            SqlTupleReader.ColumnLayoutBuilder layoutBuilder)
        {
            this.sqlQuery = sqlQuery;
            this.layoutBuilder = layoutBuilder;

            for (List<PhysColumn> keyList : layoutBuilder.keyListList) {
                addListToFrom(keyList);
            }
        }

        public final void addListToFrom(List<? extends PhysExpr> exprList) {
            for (PhysExpr expr : exprList) {
                addToFrom(expr);
            }
        }

        public void addToFrom(PhysExpr expr) {
            expr.foreachColumn(
                new Util.Function1<PhysColumn, Void>() {
                    public Void apply(PhysColumn column) {
                        addRelation(column.relation);
                        return null;
                    }
                }
            );
        }

        final void addRelation(PhysRelation relation) {
            addRelation(relation, true);
        }

        void addRelation(PhysRelation relation, boolean autoLink) {
            if (relations.contains(relation)) {
                return;
            }
            sqlQuery.addFrom(
                relation,
                relation.getAlias(),
                false);
            if (autoLink) {
                if (!relations.isEmpty()) {
                    try {
                        final PhysPath path =
                            relation.getSchema().getGraph().findPath(
                                relation, relations, false);
                        path.addToFrom(sqlQuery, false);
                        for (PhysHop hop : path.hopList) {
                            relations.add(hop.relation);
                        }
                    } catch (PhysSchemaException e) {
                        throw Util.newInternal(
                            e,
                            "While adding relation " + relation + " to query");
                    }
                }
            }
            relations.add(relation);
        }

        public final Dialect getDialect() {
            return sqlQuery.getDialect();
        }

        int addColumn(
            PhysColumn column, SqlMemberSource.Sgo sgo)
        {
            if (column == null) {
                return -1;
            }
            final String expString = column.toSql();
            int ordinal = layoutBuilder.lookup(expString);
            if (ordinal >= 0) {
                switch (sgo) {
                case SELECT_GROUP_ORDER:
                case SELECT_ORDER:
                    if (!orderBitset.get(ordinal)) {
                        sqlQuery.addOrderBy(expString, true, false, true);
                        orderBitset.set(ordinal);
                    }
                }
                return ordinal;
            }
            addToFrom(column);
            final String alias;
            switch (sgo) {
            case SELECT:
                alias = sqlQuery.addSelect(expString, column.getInternalType());
                break;
            case SELECT_GROUP:
                alias = sqlQuery.addSelectGroupBy(
                    expString, column.getInternalType());
                break;
            case SELECT_ORDER:
                sqlQuery.addOrderBy(expString, true, false, true);
                alias = sqlQuery.addSelect(expString, column.getInternalType());
                break;
            case SELECT_GROUP_ORDER:
                sqlQuery.addOrderBy(expString, true, false, true);
                alias = sqlQuery.addSelectGroupBy(
                    expString, column.getInternalType());
                break;
            default:
                throw Util.unexpected(sgo);
            }
            ordinal = layoutBuilder.register(expString, alias);
            switch (sgo) {
            case SELECT_GROUP_ORDER:
            case SELECT_ORDER:
                sqlQuery.addOrderBy(expString, true, false, true);
                orderBitset.set(ordinal);
            }
            return ordinal;
        }
    }

    /**
     * Provides and caches statistics of relations and columns.
     *
     * <p>Wrapper around a chain of {@link mondrian.spi.StatisticsProvider}s,
     * followed by a cache to store the results.</p>
     */
    public static class PhysStatistic {
        private final Map<List, Integer> columnMap =
            new HashMap<List, Integer>();
        private final Map<List, Integer> tableMap =
            new HashMap<List, Integer>();
        private final Map<String, Integer> queryMap =
            new HashMap<String, Integer>();
        private final Dialect dialect;
        private final Statement internalStatement;
        private final DataSource dataSource;

        PhysStatistic(
            Dialect dialect,
            RolapConnection internalConnection)
        {
            this.dialect = dialect;
            this.internalStatement = internalConnection.getInternalStatement();
            this.dataSource = internalConnection.getDataSource();
        }

        public int getRelationCardinality(
            RolapSchema.PhysRelation relation,
            String alias,
            int approxRowCount)
        {
            if (approxRowCount >= 0) {
                return approxRowCount;
            }
            if (relation instanceof MondrianDef.Table) {
                final MondrianDef.Table table = (MondrianDef.Table) relation;
                return getTableCardinality(
                    null, table.schema, table.name);
            } else {
                final SqlQuery sqlQuery = new SqlQuery(dialect);
                sqlQuery.addSelect("1", null);
                sqlQuery.addFrom(relation, null, true);
                return getQueryCardinality(sqlQuery.toString());
            }
        }

        private int getTableCardinality(
            String catalog,
            String schema,
            String table)
        {
            final List<String> key = Arrays.asList(catalog, schema, table);
            int rowCount = -1;
            if (tableMap.containsKey(key)) {
                rowCount = tableMap.get(key);
            } else {
                final Dialect dialect = this.dialect;
                final List<StatisticsProvider> statisticsProviders =
                    dialect.getStatisticsProviders();
                final Execution execution =
                    new Execution(internalStatement, 0);
                for (StatisticsProvider statisticsProvider
                    : statisticsProviders)
                {
                    rowCount = statisticsProvider.getTableCardinality(
                        dialect,
                        dataSource,
                        catalog,
                        schema,
                        table,
                        execution);
                    if (rowCount >= 0) {
                        break;
                    }
                }

                // Note: If all providers fail, we put -1 into the cache,
                // to ensure that we won't try again.
                tableMap.put(key, rowCount);
            }
            return rowCount;
        }

        private int getQueryCardinality(String sql) {
            int rowCount = -1;
            if (queryMap.containsKey(sql)) {
                rowCount = queryMap.get(sql);
            } else {
                final Dialect dialect = this.dialect;
                final List<StatisticsProvider> statisticsProviders =
                    dialect.getStatisticsProviders();
                final Execution execution =
                    new Execution(
                        internalStatement,
                        0);
                for (StatisticsProvider statisticsProvider
                    : statisticsProviders)
                {
                    rowCount = statisticsProvider.getQueryCardinality(
                        dialect, dataSource, sql, execution);
                    if (rowCount >= 0) {
                        break;
                    }
                }

                // Note: If all providers fail, we put -1 into the cache,
                // to ensure that we won't try again.
                queryMap.put(sql, rowCount);
            }
            return rowCount;
        }

        public int getColumnCardinality(
            RolapSchema.PhysRelation relation,
            RolapSchema.PhysExpr expression,
            int approxCardinality)
        {
            if (approxCardinality >= 0) {
                return approxCardinality;
            }
            if (relation instanceof RolapSchema.PhysTable
                && expression instanceof RolapSchema.PhysRealColumn)
            {
                final RolapSchema.PhysTable table =
                    (RolapSchema.PhysTable) relation;
                final RolapSchema.PhysColumn column =
                    (RolapSchema.PhysColumn) expression;
                return getColumnCardinality(
                    null,
                    table.getSchemaName(),
                    table.name,
                    column.name);
            } else {
                final SqlQuery sqlQuery = new SqlQuery(dialect);
                sqlQuery.setDistinct(true);
                sqlQuery.addSelect(expression.toSql(), null);
                sqlQuery.addFrom(relation, null, true);
                return getQueryCardinality(sqlQuery.toString());
            }
        }

        private int getColumnCardinality(
            String catalog,
            String schema,
            String table,
            String column)
        {
            final List<String> key =
                Arrays.asList(catalog, schema, table, column);
            int rowCount = -1;
            if (columnMap.containsKey(key)) {
                rowCount = columnMap.get(key);
            } else {
                final List<StatisticsProvider> statisticsProviders =
                    dialect.getStatisticsProviders();
                final Execution execution =
                    new Execution(
                        internalStatement,
                        0);
                for (StatisticsProvider statisticsProvider
                    : statisticsProviders)
                {
                    rowCount = statisticsProvider.getColumnCardinality(
                        dialect,
                        dataSource,
                        catalog,
                        schema,
                        table,
                        column,
                        execution);
                    if (rowCount >= 0) {
                        break;
                    }
                }

                // Note: If all providers fail, we put -1 into the cache,
                // to ensure that we won't try again.
                columnMap.put(key, rowCount);
            }
            return rowCount;
        }
    }
}

// End RolapSchema.java
