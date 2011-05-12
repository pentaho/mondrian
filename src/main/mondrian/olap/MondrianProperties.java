// Generated from MondrianProperties.xml.
package mondrian.olap;

import org.eigenbase.util.property.*;

/**
 * Configuration properties that determine the
 * behavior of a mondrian instance.
 *
 * <p>There is a method for property valid in a
 * <code>mondrian.properties</code> file. Although it is possible to retrieve
 * properties using the inherited {@link java.util.Properties#getProperty(String)}
 * method, we recommend that you use methods in this class.</p>
 *
 * @version $Id$
 */
public class MondrianProperties extends MondrianPropertiesBase {
    /**
     * <p>String property that is the AggRule element's tag value.</p>
     *
     * <p>Normally, this property is not set by a user.</p>
     */
    public transient final StringProperty AggregateRuleTag =
        new StringProperty(
            this, "mondrian.rolap.aggregates.rule.tag", "default");

    /**
     * <p>String property containing the name of the file which defines the
     * rules for recognizing an aggregate table. Can be either a resource in
     * the Mondrian jar or a URL.</p>
     *
     * <p>The default value is "/DefaultRules.xml", which is in the
     * mondrian.rolap.aggmatcher package in Mondrian.jar.</p>
     *
     * <p>Normally, this property is not set by a user.</p>
     */
    public transient final StringProperty AggregateRules =
        new StringProperty(
            this, "mondrian.rolap.aggregates.rules", "/DefaultRules.xml");

    /**
     * <p>Alerting action to take in case native evaluation of a function is
     * enabled but not supported for that function's usage in a particular
     * query.  (No alert is ever raised in cases where native evaluation would
     * definitely have been wasted effort.)</p>
     *
     * <p>Recognized actions:</p>
     *
     * <ul>
     * <li><code>OFF</code>:  do nothing (default action, also used if
     * unrecognized action is specified)</li>
     * <li><code>WARN</code>:  log a warning to RolapUtil logger</li>
     * <li><code>ERROR</code>:  throw an instance of
     * {@link NativeEvaluationUnsupportedException}</li>
     * </ul>
     */
    public transient final StringProperty AlertNativeEvaluationUnsupported =
        new StringProperty(
            this, "mondrian.native.unsupported.alert", "OFF");

    /**
     * Boolean property that controls whether the MDX parser resolves uses
     * case-sensitive matching when looking up identifiers. The default is
     * false.
     */
    public transient final BooleanProperty CaseSensitive =
        new BooleanProperty(
            this, "mondrian.olap.case.sensitive", false);

    /**
     * Property that contains the URL of the catalog to be used by
     * {@link mondrian.tui.CmdRunner} and XML/A Test.
     */
    public transient final StringProperty CatalogURL =
        new StringProperty(
            this, "mondrian.catalogURL", null);

    /**
     * <p>Boolean property that controls whether aggregate tables
     * are ordered by their volume or row count.</p>
     *
     * <p>If true, Mondrian uses the aggregate table with the smallest volume
     * (number of rows multiplied by number of columns); if false, Mondrian
     * uses the aggregate table with the fewest rows.</p>
     */
    public transient final BooleanProperty ChooseAggregateByVolume =
        new BooleanProperty(
            this, "mondrian.rolap.aggregates.ChooseByVolume", false);

    /**
     * Boolean property that controls whether sibling members are
     * compared according to order key value fetched from their ordinal
     * expression.  The default is false (only database ORDER BY is used).
     */
    public transient final BooleanProperty CompareSiblingsByOrderKey =
        new BooleanProperty(
            this, "mondrian.rolap.compareSiblingsByOrderKey", false);

    /**
     * <p>Property that defines
     * when to apply the crossjoin optimization algorithm.</p>
     *
     * <p>If a crossjoin input list's size is larger than this property's
     * value and the axis has the "NON EMPTY" qualifier, then
     * the crossjoin non-empty optimizer is applied.
     * Setting this value to '0' means that for all crossjoin
     * input lists in non-empty axes will have the optimizer applied.
     * On the other hand, if the value is set larger than any possible
     * list, say <code>Integer.MAX_VALUE</code>, then the optimizer
     * will never be applied.</p>
     */
    public transient final IntegerProperty CrossJoinOptimizerSize =
        new IntegerProperty(
            this, "mondrian.olap.fun.crossjoin.optimizer.size", 0);

    /**
     * Property that defines
     * the name of the plugin class that resolves data source names to
     * {@link javax.sql.DataSource} objects. The class must implement the
     * {@link mondrian.spi.DataSourceResolver} interface. If not specified,
     * the default implementation uses JNDI to perform resolution.
     */
    public transient final StringProperty DataSourceResolverClass =
        new StringProperty(
            this, "mondrian.spi.dataSourceResolverClass", null);

    /**
     * Boolean property that controls whether a RolapStar's
     * aggregate data cache is cleared after each query.
     * If true, no RolapStar will cache aggregate data from one
     * query to the next (the cache is cleared after each query).
     */
    public transient final BooleanProperty DisableCaching =
        new BooleanProperty(
            this, "mondrian.rolap.star.disableCaching", false);

    /**
     * Property that controls
     * whether aggregation cache hit / miss counters will be enabled.
     */
    public transient final BooleanProperty EnableCacheHitCounters =
        new BooleanProperty(
            this, "mondrian.rolap.agg.enableCacheHitCounters", false);

    /**
     * Boolean property that controls whether to use a cache for frequently
     * evaluated expressions. With the cache disabled, an expression like
     * <code>Rank([Product].CurrentMember,
     * Order([Product].MEMBERS, [Measures].[Unit Sales]))</code> would perform
     * many redundant sorts. The default is true.
     */
    public transient final BooleanProperty EnableExpCache =
        new BooleanProperty(
            this, "mondrian.expCache.enable", true);

    /**
     * <p>Property that defines
     * whether to generate SQL queries using the <code>GROUPING SETS</code>
     * construct for rollup. By default it is not enabled.</p>
     *
     * <p>Ignored on databases which do not support the
     * <code>GROUPING SETS</code> construct (see
     * {@link mondrian.spi.Dialect#supportsGroupingSets}).</p>
     */
    public transient final BooleanProperty EnableGroupingSets =
        new BooleanProperty(
            this, "mondrian.rolap.groupingsets.enable", false);

    /**
     * If enabled some NON EMPTY CrossJoin will be computed in SQL.
     */
    public transient final BooleanProperty EnableNativeCrossJoin =
        new BooleanProperty(
            this, "mondrian.native.crossjoin.enable", true);

    /**
     * If enabled some Filter() will be computed in SQL.
     */
    public transient final BooleanProperty EnableNativeFilter =
        new BooleanProperty(
            this, "mondrian.native.filter.enable", true);

    /**
     * <p>If enabled some NON EMPTY set operations like member.children,
     * level.members and member descendants will be computed in SQL.</p>
     */
    public transient final BooleanProperty EnableNativeNonEmpty =
        new BooleanProperty(
            this, "mondrian.native.nonempty.enable", true);

    /**
     * If enabled some TopCount will be computed in SQL.
     */
    public transient final BooleanProperty EnableNativeTopCount =
        new BooleanProperty(
            this, "mondrian.native.topcount.enable", true);

    /**
     * Boolean property that controls whether each query axis implicit has the
     * NON EMPTY option set. The default is false.
     */
    public transient final BooleanProperty EnableNonEmptyOnAllAxis =
        new BooleanProperty(
            this, "mondrian.rolap.nonempty", false);

    /**
     * <p>Property that determines whether to cache RolapCubeMember objects,
     * each of which associates a member of a shared hierarchy with a
     * particular cube in which it is being used.</p>
     *
     * <p>The default is {@code true}, that is, use a cache. If you wish to use
     * the member cache control aspects of {@link mondrian.olap.CacheControl},
     * you must set this property to {@code false}.</p>
     *
     * <p>RolapCubeMember has recently become more lightweight to
     * construct, and we may obsolete this cache and this
     * property.</p>
     */
    public transient final BooleanProperty EnableRolapCubeMemberCache =
        new BooleanProperty(
            this, "mondrian.rolap.EnableRolapCubeMemberCache", true);

    /**
     * If enabled, first row in the result of an XML/A drill-through request
     * will be filled with the total count of rows in underlying database.
     */
    public transient final BooleanProperty EnableTotalCount =
        new BooleanProperty(
            this, "mondrian.xmla.drillthroughTotalCount.enable", true);

    /**
     * <p>Boolean property that controls whether to notify the Mondrian system
     * when a {@link MondrianProperties property value} changes.</p>
     *
     * <p>This allows objects dependent on Mondrian properties to react (that
     * is, reload), when a given property changes via, say,
     * <code>MondrianProperties.instance().populate(null)</code> or
     * <code>MondrianProperties.instance().QueryLimit.set(50)</code>.</p>
     */
    public transient final BooleanProperty EnableTriggers =
        new BooleanProperty(
            this, "mondrian.olap.triggers.enable", true);

    /**
     * <p>Property that defines
     * the name of the class used to compile scalar expressions.</p>
     *
     * <p>If the value is
     * non-null, it is used by the <code>ExpCompiler.Factory</code>
     * to create the implementation.</p>
     */
    public transient final StringProperty ExpCompilerClass =
        new StringProperty(
            this, "mondrian.calc.ExpCompiler.class", null);

    /**
     * When looking for native evaluation of an expression, expand non native
     * subexpressions into MemberLists.
     */
    public transient final BooleanProperty ExpandNonNative =
        new BooleanProperty(
            this, "mondrian.native.ExpandNonNative", false);

    /**
     * <p>Property that defines
     * whether to generate joins to filter out members in a snowflake
     * dimension that do not have any children.</p>
     *
     * <p>If true (the default), some queries to query members of high
     * levels snowflake dimensions will be more expensive. If false, and if
     * there are rows in an outer snowflake table that are not referenced by
     * a row in an inner snowflake table, then some queries will return members
     * that have no children.</p>
     *
     * <p>Our recommendation, for best performance, is to remove rows outer
     * snowflake tables are not referenced by any row in an inner snowflake
     * table, during your ETL process, and to set this property to
     * {@code false}.</p>
     */
    public transient final BooleanProperty FilterChildlessSnowflakeMembers =
        new BooleanProperty(
            this, "mondrian.rolap.FilterChildlessSnowflakeMembers", true);

    /**
     * Property containing the JDBC URL of the FoodMart database.
     * The default value is to connect to an ODBC data source called
     * "MondrianFoodMart".
     */
    public transient final StringProperty FoodmartJdbcURL =
        new StringProperty(
            this, "mondrian.foodmart.jdbcURL", "jdbc:odbc:MondrianFoodMart");

    /**
     * <p>Boolean property that controls whether to print the SQL code
     * generated for aggregate tables.</p>
     *
     * <p>If set, then as each aggregate request is processed, both the lost
     * and collapsed dimension create and insert sql code is printed.
     * This is for use in the CmdRunner allowing one to create aggregate table
     * generation sql.</p>
     */
    public transient final BooleanProperty GenerateAggregateSql =
        new BooleanProperty(
            this, "mondrian.rolap.aggregates.generateSql", false);

    /**
     * Boolean property that controls pretty-print mode.
     * If set to true, the all SqlQuery SQL strings
     * will be generated in pretty-print mode, formatted for ease of reading.
     */
    public transient final BooleanProperty GenerateFormattedSql =
        new BooleanProperty(
            this, "mondrian.rolap.generate.formatted.sql", false);

    /**
     * Property that establishes the amount of chunks for querying cells
     * involving high-cardinality dimensions.
     * Should prime with {@link #ResultLimit mondrian.result.limit}.
     */
    public transient final IntegerProperty HighCardChunkSize =
        new IntegerProperty(
            this, "mondrian.result.highCardChunkSize", 1);

    /**
     * Property that defines
     * whether non-existent member errors should be ignored during schema
     * load.
     */
    public transient final BooleanProperty IgnoreInvalidMembers =
        new BooleanProperty(
            this, "mondrian.rolap.ignoreInvalidMembers", false);

    /**
     * Property that defines
     * whether non-existent member errors should be ignored during query
     * validation.
     */
    public transient final BooleanProperty IgnoreInvalidMembersDuringQuery =
        new BooleanProperty(
            this, "mondrian.rolap.ignoreInvalidMembersDuringQuery", false);

    /**
     * <p>Property that defines whether to ignore measure when non joining
     * dimension is in the tuple during aggregation.</p>
     *
     * <p>If there are unrelated dimensions to a measure in context during
     * aggregation, the measure is ignored in the evaluation context. This
     * behaviour kicks in only if the cubeusage for this measure has
     * IgnoreUnrelatedDimensions attribute set to false.</p>
     *
     * <p>For example, Gender doesn't join with [Warehouse Sales] measure.</p>
     *
     * <p>With mondrian.olap.agg.IgnoreMeasureForNonJoiningDimension=true
     * Warehouse Sales gets eliminated and is ignored in the aggregate
     * value.</p>
     *
     * <blockquote>
     * <p>                                    [Store Sales] + [Warehouse Sales]
     * SUM({Product.members * Gender.members})    7,913,333.82</p>
     * </blockquote>
     * <p>With mondrian.olap.agg.IgnoreMeasureForNonJoiningDimension=false
     * Warehouse Sales with Gender All level member contributes to the aggregate
     * value.</p>
     * <blockquote>
     * <p>                                     [Store Sales] + [Warehouse Sales]
     * SUM({Product.members * Gender.members})    9,290,730.03</p>
     * </blockquote>
     * <p>On a report where Gender M, F and All members exist a user will see a
     * large aggregated value compared to the aggregated value that can be
     * arrived at by suming up values against Gender M and F. This can be
     * confusing to the user. This feature can be used to eliminate such a
     * situation.</p>
     */
    public transient final BooleanProperty IgnoreMeasureForNonJoiningDimension =
        new BooleanProperty(
            this, "mondrian.olap.agg.IgnoreMeasureForNonJoiningDimension",
            false);

    /**
     * <p>Property that defines the iteration limit when computing an
     * aggregate; 0 indicates unlimited.</p>
     */
    public transient final IntegerProperty IterationLimit =
        new IntegerProperty(
            this, "mondrian.rolap.iterationLimit", 0);

    /**
     * Not documented.
     */
    public transient final IntegerProperty Iterations =
        new IntegerProperty(
            this, "mondrian.test.Iterations", 1);

    /**
     * Property containing a list of JDBC drivers to load automatically.
     * Must be a comma-separated list of class names, and the classes must be
     * on the class path.
     */
    public transient final StringProperty JdbcDrivers =
        new StringProperty(
            this, "mondrian.jdbcDrivers",
            "sun.jdbc.odbc.JdbcOdbcDriver,org.hsqldb.jdbcDriver,oracle.jdbc.OracleDriver,com.mysql.jdbc.Driver");

    /**
     * <p>Property that defines the JdbcSchema factory class which
     * determines the list of tables and columns of a specific datasource.</p>
     *
     * @see mondrian.rolap.aggmatcher.JdbcSchema
     */
    public transient final StringProperty JdbcFactoryClass =
        new StringProperty(
            this, "mondrian.rolap.aggregates.jdbcFactoryClass", null);

    /**
     * <p>String property that holds the
     * name of the class whose resource bundle is to be used to for this
     * schema. For example, if the class is {@code com.acme.MyResource},
     * mondrian will look for a resource bundle called
     * {@code com/acme/MyResource_<i>locale</i>.properties} on the class path.
     * (This property has a confusing name because in a previous release it
     * actually held a file name.)</p>
     *
     * <p>Used for the {@link mondrian.i18n.LocalizingDynamicSchemaProcessor};
     * see <a>Internationalization</a>
     * for more details.</p>
     *
     * <p>Default value is null.</p>
     */
    public transient final StringProperty LocalePropFile =
        new StringProperty(
            this, "mondrian.rolap.localePropFile", null);

    /**
     * <p>Max number of constraints in a single 'IN' SQL clause.</p>
     *
     * <p>This value may be variant among database prodcuts and their runtime
     * settings. Oracle, for example, gives the error "ORA-01795: maximum
     * number of expressions in a list is 1000".</p>
     *
     * <p>Recommended values:</p>
     * <ul>
     * <li>Oracle: 1,000</li>
     * <li>DB2: 2,500</li>
     * <li>Other: 10,000</li>
     * </ul>
     */
    public transient final IntegerProperty MaxConstraints =
        new IntegerProperty(
            this, "mondrian.rolap.maxConstraints", 1000);

    /**
     * <p>Boolean property that defines the maximum number of passes
     * allowable while evaluating an MDX expression.</p>
     *
     * <p>If evaluation exceeds this depth (for example, while evaluating a
     * very complex calculated member), Mondrian will throw an error.</p>
     */
    public transient final IntegerProperty MaxEvalDepth =
        new IntegerProperty(
            this, "mondrian.rolap.evaluate.MaxEvalDepth", 10);

    /**
     * Property that defines
     * limit on the number of rows returned by XML/A drill through request.
     */
    public transient final IntegerProperty MaxRows =
        new IntegerProperty(
            this, "mondrian.xmla.drillthroughMaxRows", 1000);

    /**
     * <p>Property that defines
     * whether the <code>MemoryMonitor</code> should be enabled. By
     * default for Java5 and above it is not enabled.</p>
     */
    public transient final BooleanProperty MemoryMonitor =
        new BooleanProperty(
            this, "mondrian.util.memoryMonitor.enable", false);

    /**
     * <p>Property that defines
     * the name of the class used as a memory monitor.</p>
     *
     * <p>If the value is
     * non-null, it is used by the <code>MemoryMonitorFactory</code>
     * to create the implementation.</p>
     */
    public transient final StringProperty MemoryMonitorClass =
        new StringProperty(
            this, "mondrian.util.MemoryMonitor.class", null);

    /**
     * <p>Property that defines
     * the default <code>MemoryMonitor</code> percentage threshold.</p>
     */
    public transient final IntegerProperty MemoryMonitorThreshold =
        new IntegerProperty(
            this, "mondrian.util.memoryMonitor.percentage.threshold", 90);

    /**
     * Not documented.
     */
    public transient final IntegerProperty NativizeMaxResults =
        new IntegerProperty(
            this, "mondrian.native.NativizeMaxResults", 150000);

    /**
     * Not documented.
     */
    public transient final IntegerProperty NativizeMinThreshold =
        new IntegerProperty(
            this, "mondrian.native.NativizeMinThreshold", 100000);

    /**
     * <p>Property determines if elements of dimension (levels, hierarchies,
     * members) need to be prefixed with dimension name in MDX query.</p>
     *
     * <p>For example when the property is true, the following queries
     * will error out. The same queries will work when this property
     * is set to false.</p>
     *
     * <blockquote>select {[M]} on 0 from sales<br></br>
     * select {[USA]} on 0 from sales<br></br>
     * select {[USA].[CA].[Santa Monica]}  on 0 from sales</blockquote>
     *
     * <p>When the property is set to true, any query where elements are
     * prefixed with dimension name as below will work</p>
     *
     * <blockquote>select {[Gender].[F]} on 0 from sales<br></br>
     * select {[Customers].[Santa Monica]} on 0 from sales</blockquote>
     *
     * <p>Please note that this property does not govern the behaviour
     * wherein</p>
     *
     * <blockquote>[Gender].[M]</blockquote>
     *
     * <p>is resolved into a fully qualified</p>
     *
     * <blockquote>[Gender].[M]</blockquote>
     *
     * <p> In a scenario where the schema is very large and dimensions have
     * large number of members a MDX query that has a invalid member in it will
     * cause mondrian to to go through all the dimensions, levels, hierarchies,
     * members and properties trying to resolve the element name. This behavior
     * consumes considerable time and resources on the server. Setting this
     * property to true will make it fail fast in a scenario where it is
     * desirable.</p>
     */
    public transient final BooleanProperty NeedDimensionPrefix =
        new BooleanProperty(
            this, "mondrian.olap.elements.NeedDimensionPrefix", false);

    /**
     * <p>Property that defines
     * the behavior of division if the denominator evaluates to zero.</p>
     *
     * <p>If a division has a non-null numerator and a null denominator,
     * it evaluates to "Infinity", which conforms to MSAS behavior. However,
     * the old semantics of evaluating this to NULL (non MSAS-conforming), is
     * useful in some applications. This property controls whether the
     * result should be NULL if the denominator is Null.</p>
     */
    public transient final BooleanProperty NullDenominatorProducesNull =
        new BooleanProperty(
            this, "mondrian.olap.NullDenominatorProducesNull", false);

    /**
     * <p>Property that determines how a null member value is represented in the
     * result output.</p>
     * <p>AS 2000 shows this as empty value</p>
     * <p>AS 2005 shows this as "(null)" value</p>
     */
    public transient final StringProperty NullMemberRepresentation =
        new StringProperty(
            this, "mondrian.olap.NullMemberRepresentation", "#null");

    /**
     * Boolean property that determines whether Mondrian optimizes predicates.
     */
    public transient final BooleanProperty OptimizePredicates =
        new BooleanProperty(
            this, "mondrian.rolap.aggregates.optimizePredicates", true);

    /**
     * <p>Property that defines the name of the factory class used
     * to create maps of member properties to their respective values.</p>
     *
     * <p>If the value is
     * non-null, it is used by the <code>PropertyValueFactory</code>
     * to create the implementation.  If unset,
     * {@link mondrian.rolap.RolapMemberBase.DefaultPropertyValueMapFactory}
     * will be used.</p>
     */
    public transient final StringProperty PropertyValueMapFactoryClass =
        new StringProperty(
            this, "mondrian.rolap.RolapMember.PropertyValueMapFactory.class",
            null);

    /**
     * Property defining
     * where the test XML files are.
     */
    public transient final StringProperty QueryFileDirectory =
        new StringProperty(
            this, "mondrian.test.QueryFileDirectory", null);

    /**
     * Property that defines
     * a pattern for which test XML files to run.  Pattern has to
     * match a file name of the form:
     * <code>query<i>whatever</i>.xml</code> in the directory.
     *
     * <p>Example:</p>
     *
     * <blockquote><code>
     * mondrian.test.QueryFilePattern=queryTest_fec[A-Za-z0-9_]*.xml
     * </code></blockquote>
     */
    public transient final StringProperty QueryFilePattern =
        new StringProperty(
            this, "mondrian.test.QueryFilePattern", null);

    /**
     * <p>Maximum number of simultaneous queries the system will allow.</p>
     *
     * <p>Oracle fails if you try to run more than the 'processes' parameter in
     * init.ora, typically 150. The throughput of Oracle and other databases
     * will probably reduce long before you get to their limit.</p>
     */
    public transient final IntegerProperty QueryLimit =
        new IntegerProperty(
            this, "mondrian.query.limit", 40);

    /**
     * Property that defines
     * the timeout value (in seconds) for queries; 0, the default, indicates no
     * timeout.
     */
    public transient final IntegerProperty QueryTimeout =
        new IntegerProperty(
            this, "mondrian.rolap.queryTimeout", 0);

    /**
     * <p>Boolean property that determines whether Mondrian should read
     * aggregate tables.</p>
     *
     * <p>If set to true, then Mondrian scans the database for aggregate tables.
     * Unless mondrian.rolap.aggregates.Use is set to true, the aggregates
     * found will not be used.</p>
     */
    public transient final BooleanProperty ReadAggregates =
        new BooleanProperty(
            this, "mondrian.rolap.aggregates.Read", false);

    /**
     * Integer property that, if set to a value greater than zero, limits the
     * maximum size of a result set.
     */
    public transient final IntegerProperty ResultLimit =
        new IntegerProperty(
            this, "mondrian.result.limit", 0);

    /**
     * Property which defines which SegmentCache implementation to use.
     * Specify the value as a fully qualified class name, such as
     * <code>org.example.SegmentCacheImpl</code> where SegmentCacheImpl
     * is an implementation of {@link mondrian.spi.SegmentCache}.
     */
    public transient final StringProperty SegmentCache =
        new StringProperty(
            this, "mondrian.rolap.SegmentCache", null);

    /**
     * <p>Property which defines the timeout for
     * {@link mondrian.spi.SegmentCache#contains(mondrian.rolap.agg.SegmentHeader)}
     * in milliseconds. Defaults to 5000.</p>
     *
     * <p>This is an internal control property. The timeout value
     * won't be passed to the underlying
     * {@link mondrian.spi.SegmentCache} SPI.</p>
     */
    public transient final IntegerProperty SegmentCacheLookupTimeout =
        new IntegerProperty(
            this, "mondrian.rolap.SegmentCacheLookupTimeout", 5000);

    /**
     * <p>Property which defines the timeout for
     * {@link mondrian.spi.SegmentCache#get(mondrian.rolap.agg.SegmentHeader)}
     * in milliseconds. Defaults to 5000.</p>
     *
     * <p>This is an internal control property. The timeout value
     * won't be passed to the underlying
     * {@link mondrian.spi.SegmentCache} SPI.</p>
     */
    public transient final IntegerProperty SegmentCacheReadTimeout =
        new IntegerProperty(
            this, "mondrian.rolap.SegmentCacheReadTimeout", 5000);

    /**
     * <p>Property which defines the timeout for
     * {@link mondrian.spi.SegmentCache#getSegmentHeaders()}
     * in milliseconds. Defaults to 5000.</p>
     *
     * <p>This is an internal control property. The timeout value
     * won't be passed to the underlying
     * {@link mondrian.spi.SegmentCache} SPI.</p>
     */
    public transient final IntegerProperty SegmentCacheScanTimeout =
        new IntegerProperty(
            this, "mondrian.rolap.SegmentCacheScanTimeout", 5000);

    /**
     * <p>Property which defines the timeout for
     * {@link mondrian.spi.SegmentCache#put(mondrian.rolap.agg.SegmentHeader, mondrian.rolap.agg.SegmentBody)}
     * in milliseconds. Defaults to 5000.</p>
     *
     * <p>This is an internal control property. The timeout value
     * won't be passed to the underlying
     * {@link mondrian.spi.SegmentCache} SPI.</p>
     */
    public transient final IntegerProperty SegmentCacheWriteTimeout =
        new IntegerProperty(
            this, "mondrian.rolap.SegmentCacheWriteTimeout", 5000);

    /**
     * Property that controls the behavior of
     * {@link Property#SOLVE_ORDER solve order} of calculated members and sets.
     *
     * <p>Valid values are "absolute" and "scoped" (the default). See
     * {@link mondrian.olap.SolveOrderMode} for details.</p>
     */
    public transient final StringProperty SolveOrderMode =
        new StringProperty(
            this, "mondrian.rolap.SolveOrderMode", "ABSOLUTE");

    /**
     * <p>Property that, with {@link #SparseSegmentDensityThreshold}, determines
     * whether to choose a sparse or dense representation when storing
     * collections of cell values in memory.</p>
     *
     * <p>When storing collections of cell values, Mondrian has to choose
     * between a sparse and a dense representation, based upon the
     * <code>possible</code> and <code>actual</code> number of values.
     * The <code>density</code> is <code>actual / possible</code>.</p>
     *
     * <p>We use a sparse representation if
     * <code>(possible -
     * {@link #SparseSegmentCountThreshold countThreshold}) *
     * {@link #SparseSegmentDensityThreshold densityThreshold} >
     * actual</code></p>
     *
     * <p>For example, at the default values
     * ({@link #SparseSegmentCountThreshold countThreshold} = 1000,
     * {@link #SparseSegmentDensityThreshold} = 0.5),
     * we use a dense representation for</p>
     *
     * <ul>
     * <li>(1000 possible, 0 actual), or</li>
     * <li>(2000 possible, 500 actual), or</li>
     * <li>(3000 possible, 1000 actual).</li>
     * </ul>
     *
     * <p>Any fewer actual values, or any more
     * possible values, and Mondrian will use a sparse representation.</p>
     */
    public transient final IntegerProperty SparseSegmentCountThreshold =
        new IntegerProperty(
            this, "mondrian.rolap.SparseSegmentValueThreshold", 1000);

    /**
     * Property that, with {@link #SparseSegmentCountThreshold},
     * determines whether to choose a sparse or dense representation when
     * storing collections of cell values in memory.
     */
    public transient final DoubleProperty SparseSegmentDensityThreshold =
        new DoubleProperty(
            this, "mondrian.rolap.SparseSegmentDensityThreshold", 0.5);

    /**
     * <p>Property that defines the name of the class used in SqlMemberSource
     * to pool common values.</p>
     *
     * <p>If the value is non-null, it is used by the
     * <code>SqlMemberSource.ValueMapFactory</code>
     * to create the implementation.  If it is not set, then
     * {@link mondrian.rolap.SqlMemberSource.NullValuePoolFactory}
     * will be used, meaning common values will not be pooled.</p>
     */
    public transient final StringProperty SqlMemberSourceValuePoolFactoryClass =
        new StringProperty(
            this, "mondrian.rolap.SqlMemberSource.ValuePoolFactory.class",
            null);

    /**
     * <p>Property that defines
     * whether to enable new naming behavior.</p>
     *
     * <p>If true, hierarchies are named [Dimension].[Hierarchy]; if false,
     * [Dimension.Hierarchy].</p>
     */
    public transient final BooleanProperty SsasCompatibleNaming =
        new BooleanProperty(
            this, "mondrian.olap.SsasCompatibleNaming", false);

    /**
     * <p>String property that determines which test class to run.</p>
     *
     * <p>This is the name of the class which either implements
     * {@code junit.framework.Test} or has a method
     * {@code public [static] junit.framework.Test suite()}.</p>
     *
     * <p>Example:</p>
     *
     * <blockquote><code>
     * mondrian.test.Class=mondrian.test.FoodMartTestCase
     * </code></blockquote>
     *
     * @see #TestName
     */
    public transient final StringProperty TestClass =
        new StringProperty(
            this, "mondrian.test.Class", null);

    /**
     * <p>Property containing the connect string which regresssion tests should
     * use to connect to the database.</p>
     *
     * <p>Format is specified in {@link Util#parseConnectString(String)}.</p>
     */
    public transient final StringProperty TestConnectString =
        new StringProperty(
            this, "mondrian.test.connectString", null);

    /**
     * <p>Integer property that controls whether to test operators'
     * dependencies, and how much time to spend doing it.</p>
     *
     * <p>If this property is positive, Mondrian's test framework allocates an
     * expression evaluator which evaluates each expression several times, and
     * makes sure that the results of the expression are independent of
     * dimensions which the expression claims to be independent of.</p>
     *
     * <p>The default is 0.</p>
     */
    public transient final IntegerProperty TestExpDependencies =
        new IntegerProperty(
            this, "mondrian.test.ExpDependencies", 0);

    /**
     * Property containing a list of dimensions in the Sales cube that should
     * be treated as high-cardinality dimensions by the testing infrastructure.
     * This allows us to run the full suite of tests with high-cardinality
     * functionality enabled.
     */
    public transient final StringProperty TestHighCardinalityDimensionList =
        new StringProperty(
            this, "mondrian.test.highCardDimensions", null);

    /**
     * Property containing the JDBC password of a test database.
     * The default value is null, to cope with DBMSs that don't need this.
     */
    public transient final StringProperty TestJdbcPassword =
        new StringProperty(
            this, "mondrian.test.jdbcPassword", null);

    /**
     * Property containing the JDBC URL of a test database.
     * It does not default.
     */
    public transient final StringProperty TestJdbcURL =
        new StringProperty(
            this, "mondrian.test.jdbcURL", null);

    /**
     * Property containing the JDBC user of a test database.
     * The default value is null, to cope with DBMSs that don't need this.
     */
    public transient final StringProperty TestJdbcUser =
        new StringProperty(
            this, "mondrian.test.jdbcUser", null);

    /**
     * <p>String property that determines which tests are run.</p>
     *
     * <p>This is a regular expression as defined by
     * {@link java.util.regex.Pattern}.
     * If this property is specified, only tests whose names match the pattern
     * in its entirety will be run.</p>
     *
     * @see #TestClass
     */
    public transient final StringProperty TestName =
        new StringProperty(
            this, "mondrian.test.Name", null);

    /**
     * <p>Seed for random number generator used by some of the tests.</p>
     *
     * <p>Any value besides 0 or -1 gives deterministic behavior.
     * The default value is 1234: most users should use this.
     * Setting the seed to a different value can increase coverage, and
     * therefore may uncover new bugs.</p>
     *
     * <p>If you set the value to 0, the system will generate its own
     * pseudo-random seed.</p>
     *
     * <p>If you set the value to -1, Mondrian uses the next seed from an
     * internal random-number generator. This is a little more deterministic
     * than setting the value to 0.</p>
     */
    public transient final IntegerProperty TestSeed =
        new IntegerProperty(
            this, "mondrian.test.random.seed", 1234);

    /**
     * Property that returns the time limit for the test run in seconds.
     * If the test is running after that time, it is terminated.
     */
    public transient final IntegerProperty TimeLimit =
        new IntegerProperty(
            this, "mondrian.test.TimeLimit", 0);

    /**
     * <p>Boolean property that controls whether Mondrian uses aggregate
     * tables.</p>
     *
     * <p>If true, then Mondrian uses aggregate tables. This property is
     * queried prior to each aggregate query so that changing the value of this
     * property dynamically (not just at startup) is meaningful.</p>
     *
     * <p>Aggregates can be read from the database using the
     * {@link #ReadAggregates} property but will not be used unless this
     * property is set to true.</p>
     */
    public transient final BooleanProperty UseAggregates =
        new BooleanProperty(
            this, "mondrian.rolap.aggregates.Use", false);

    /**
     * Not documented.
     */
    public transient final IntegerProperty VUsers =
        new IntegerProperty(
            this, "mondrian.test.VUsers", 1);

    /**
     * Property that indicates whether this is a "warmup test".
     */
    public transient final BooleanProperty Warmup =
        new BooleanProperty(
            this, "mondrian.test.Warmup", false);

    /**
     * <p>Property that controls if warning messages should be printed if a sql
     * comparison tests do not contain expected sqls for the specified
     * dialect. The tests are skipped if no expected sqls are
     * found for the current dialect.</p>
     *
     * <p>Possible values are the following:</p>
     *
     * <ul>
     * <li>"NONE": no warning (default)</li>
     * <li>"ANY": any dialect</li>
     * <li>"ACCESS"</li>
     * <li>"DERBY"</li>
     * <li>"LUCIDDB"</li>
     * <li>"MYSQL"</li>
     * <li>... and any Dialect enum in SqlPattern.Dialect</li>
     * </ul>
     *
     * <p>Specific tests can overwrite the default setting. The priority is:<ul>
     * <li>Settings besides "ANY" in mondrian.properties file</li>
     * <li>< Any setting in the test</li>
     * <li>< "ANY"</li>
     * </ul>
     * </p>
     */
    public transient final StringProperty WarnIfNoPatternForDialect =
        new StringProperty(
            this, "mondrian.test.WarnIfNoPatternForDialect", "NONE");

    /**
     * <p>Interval, in milliseconds, at which to refresh the
     * list of XML/A catalogs. This is usually known as the
     * datasources.xml file. See also
     * {@link mondrian.xmla.impl.DynamicDatasourceXmlaServlet}.</p>
     */
    public transient final IntegerProperty XmlaSchemaRefreshInterval =
        new IntegerProperty(
            this, "mondrian.xmla.SchemaRefreshInterval", 3000);

}

// End MondrianProperties.java
